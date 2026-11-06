package history

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/stretchr/testify/require"
)

func TestPolicyAdoptionsPersistExactBoundariesAndDeepCopy(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	h, db := openTest(t, dir)
	_, err := h.AdoptPolicy(Policy{Revision: 1, Collections: []string{"app.example.one"}}, 1)
	require.NoError(t, err)
	_, err = h.Append(record("event", 1))
	require.NoError(t, err)
	_, err = h.AdoptPolicy(Policy{Revision: 2, Collections: []string{"app.example.two"}}, 2)
	require.NoError(t, err)
	_, err = h.AdoptPolicy(Policy{Revision: 3, Collections: []string{"app.example.three"}}, 3)
	require.NoError(t, err)
	adoptions, err := h.ReadPolicyAdoptions()
	require.NoError(t, err)
	require.Len(t, adoptions, 3)
	require.Equal(t, uint64(1), adoptions[0].Policy.Revision)
	require.Equal(t, uint64(0), *adoptions[0].EffectiveAfterJournalSeq)
	require.Equal(t, uint64(2), adoptions[1].Policy.Revision)
	require.Equal(t, uint64(1), *adoptions[1].EffectiveAfterJournalSeq)
	require.Equal(t, uint64(3), adoptions[2].Policy.Revision)
	require.Equal(t, uint64(1), *adoptions[2].EffectiveAfterJournalSeq)
	*adoptions[1].EffectiveAfterJournalSeq = 99
	adoptions[1].Policy.Collections[0] = "corrupted"
	again, err := h.ReadPolicyAdoptions()
	require.NoError(t, err)
	require.Equal(t, uint64(1), *again[1].EffectiveAfterJournalSeq)
	require.Equal(t, "app.example.two", again[1].Policy.Collections[0])
	require.NoError(t, db.Close())
	h, db = openTest(t, dir)
	defer db.Close()
	reopened, err := h.ReadPolicyAdoptions()
	require.NoError(t, err)
	require.Equal(t, uint64(0), *reopened[0].EffectiveAfterJournalSeq)
	require.Equal(t, uint64(1), *reopened[1].EffectiveAfterJournalSeq)
	require.Equal(t, uint64(1), *reopened[2].EffectiveAfterJournalSeq)
}

func TestStagedPolicyCommitFailureDoesNotAddAdoption(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	_, db := openTest(t, dir)
	require.NoError(t, db.Close())
	fault := store.WithFaultInjector(&store.KeyPrefixFault{Prefix: []byte(keyPolicyState), Op: store.WriteOpBatchCommit, Ordinal: 1, Err: errors.New("injected policy commit failure")})
	db, err := store.Open(dir, nil, fault)
	require.NoError(t, err)
	h, err := Open(db)
	require.NoError(t, err)
	batch := db.NewBatch()
	_, err = h.StagePolicy(batch, Policy{Revision: 1, Collections: []string{"app.example.record"}}, 1)
	require.NoError(t, err)
	require.ErrorContains(t, db.Commit(batch, store.SyncWrites), "injected policy commit failure")
	require.NoError(t, batch.Close())
	require.NoError(t, db.Close())
	h, db = openTest(t, dir)
	defer db.Close()
	adoptions, err := h.ReadPolicyAdoptions()
	require.NoError(t, err)
	require.Empty(t, adoptions)
}
