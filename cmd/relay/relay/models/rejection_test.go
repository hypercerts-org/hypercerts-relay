package models

import "testing"

func TestRejectedEventTableName(t *testing.T) {
	if got, want := (RejectedEvent{}).TableName(), "rejected_event"; got != want {
		t.Fatalf("TableName() = %q, want %q", got, want)
	}
}
