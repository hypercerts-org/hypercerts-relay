package relay

import "context"

func (sub *Subscription) setState(state string) {
	sub.lk.Lock()
	sub.state = state
	sub.lk.Unlock()
}

func (s *Slurper) SourceState(hostname string) string {
	s.subsLk.Lock()
	sub := s.subs[hostname]
	s.subsLk.Unlock()
	if sub == nil {
		return "configured"
	}
	sub.lk.RLock()
	defer sub.lk.RUnlock()
	return sub.state
}

func (s *Slurper) StopSource(ctx context.Context, hostname string) error {
	s.subsLk.Lock()
	sub := s.subs[hostname]
	if sub != nil {
		sub.cancel()
	}
	s.subsLk.Unlock()
	if sub == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-sub.done:
		return sub.finishErr
	}
}

// CancelSources closes active source contexts without waiting for their
// schedulers. Lease fencing calls this from a scheduler path, where waiting for
// source completion would deadlock the connection that must be canceled.
func (s *Slurper) CancelSources() {
	s.subsLk.Lock()
	defer s.subsLk.Unlock()
	for _, sub := range s.subs {
		sub.cancel()
	}
}
