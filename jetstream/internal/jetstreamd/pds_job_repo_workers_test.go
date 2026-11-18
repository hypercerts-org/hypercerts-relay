package jetstreamd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEffectivePDSJobRepoWorkers(t *testing.T) {
	require.Equal(t, DefaultPDSJobRepoWorkers, Options{}.effectivePDSJobRepoWorkers())
	require.Equal(t, 2, DefaultPDSJobRepoWorkers)
	require.Equal(t, 1, Options{PDSJobRepoWorkers: 1}.effectivePDSJobRepoWorkers(), "one worker can be configured explicitly")
	require.Equal(t, 8, Options{PDSJobRepoWorkers: 8}.effectivePDSJobRepoWorkers())
	require.Equal(t, MaxPDSJobRepoWorkers, Options{PDSJobRepoWorkers: 1000}.effectivePDSJobRepoWorkers())
}
