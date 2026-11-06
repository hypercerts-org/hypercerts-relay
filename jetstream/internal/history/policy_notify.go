package history

func (s *Store) NotifyAfterCommit() {
	notifyCommitted(s.db)
}
