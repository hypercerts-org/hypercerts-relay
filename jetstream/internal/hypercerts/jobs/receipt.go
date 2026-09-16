package jobs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// RelayReceiptSender submits only the bounded completion coordinate owned by
// Plan 004. It never sends inventory entries, repository content, or tokens.
type RelayReceiptSender struct {
	URL    string
	Token  string
	Client *http.Client
}

func (s RelayReceiptSender) Send(ctx context.Context, job Job) error {
	if job.SourceRevision == 0 {
		return nil
	}
	payload, err := json.Marshal(struct {
		PDS             string `json:"pds"`
		SourceRevision  uint64 `json:"sourceRevision"`
		PolicyRevision  uint64 `json:"policyRevision"`
		JobID           string `json:"jobId"`
		DurableBoundary string `json:"durableBoundary"`
	}{job.PDS, job.SourceRevision, job.Policy.Revision, job.ID, "current_state_complete"})
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(s.URL, "/")+"/hypercerts/v1/source/recovery-receipt", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+s.Token)
	request.Header.Set("Content-Type", "application/json")
	client := s.Client
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("submit recovery receipt: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		if response.StatusCode == http.StatusNotFound {
			var failure struct {
				Error string `json:"error"`
			}
			if err := json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&failure); err == nil && failure.Error == "source_not_found" {
				return ErrReceiptSourceMissing
			}
			return fmt.Errorf("submit recovery receipt: relay returned 404")
		}
		if response.StatusCode == http.StatusConflict {
			return ErrReceiptStale
		}
		return fmt.Errorf("submit recovery receipt: relay returned %d", response.StatusCode)
	}
	return nil
}
