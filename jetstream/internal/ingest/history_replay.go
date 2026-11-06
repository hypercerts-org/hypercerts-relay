package ingest

import (
	"context"
	"fmt"

	"github.com/bluesky-social/jetstream/internal/history"
)

const historyReplayPageSize = 256

func (w *Writer) drainHistoryLocked(ctx context.Context) error {
	if w.cfg.History == nil {
		return nil
	}
	return w.drainHistoryBeforeLocked(ctx, 0)
}

func (w *Writer) drainHistoryBeforeLocked(ctx context.Context, before uint64) error {
	tip, err := w.cfg.History.Tip()
	if err != nil {
		return err
	}
	applied, err := w.cfg.History.AppliedThroughFor(w.cfg.HistoryProjectionTarget)
	if err != nil {
		return err
	}
	scan := uint64(0)
	for scan < tip {
		if before != 0 && scan+1 >= before {
			return nil
		}
		pageTip := tip
		if before != 0 && pageTip >= before {
			pageTip = before - 1
		}
		page, err := w.cfg.History.Read(scan, pageTip, historyReplayPageSize)
		if err != nil {
			return err
		}
		if len(page.Events) == 0 {
			return fmt.Errorf("ingest: history replay missing sequence %d", applied+1)
		}
		for _, receipt := range page.Events {
			if before != 0 && receipt.Sequence >= before {
				return nil
			}
			scan = receipt.Sequence
			if receipt.Owner != w.cfg.HistoryProjectionTarget {
				continue
			}
			if receipt.Sequence <= applied {
				continue
			}
			status, err := w.cfg.History.ProjectionStatusFor(w.cfg.HistoryProjectionTarget, receipt.Sequence)
			if err != nil {
				return err
			}
			if status == history.RecoveryComplete {
				applied = receipt.Sequence
				continue
			}
			if receipt.Disposition == history.Apply {
				if w.cfg.Attribution == nil {
					return fmt.Errorf("ingest: history replay requires attribution manager")
				}
				metadata := attributionFromHistory(receipt.Attribution)
				if metadata.StreamID == "" || metadata.Policy.Revision == 0 || metadata.Provenance == "" {
					return fmt.Errorf("ingest: history replay missing saved attribution at sequence %d: %w", receipt.Sequence, history.ErrInvalid)
				}
				if err := w.appendProjectedLockedWithDrain(ctx, receipt.InputEvent, receipt.Sequence, metadata, false); err != nil {
					return fmt.Errorf("ingest: replay history sequence %d: %w", receipt.Sequence, err)
				}
				if err := w.flushAndRotateLocked(ctx); err != nil {
					return fmt.Errorf("ingest: flush history sequence %d: %w", receipt.Sequence, err)
				}
			} else {
				intent := history.ProjectionIntent{Target: w.cfg.HistoryProjectionTarget, JournalSequence: receipt.Sequence, Fingerprint: receipt.Source.ContentHash, Disposition: receipt.Disposition}
				if err := w.cfg.History.PrepareProjection(intent); err != nil {
					return err
				}
				w.addPendingProjection(receipt.Sequence)
				if err := w.commitDurableBatchLocked(ctx, w.durableNextSeq, true, w.sampleDurableBatchPrepareValueLocked()); err != nil {
					return err
				}
			}
			applied, err = w.cfg.History.AppliedThroughFor(w.cfg.HistoryProjectionTarget)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
