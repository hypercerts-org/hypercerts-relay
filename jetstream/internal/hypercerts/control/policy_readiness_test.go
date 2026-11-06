package control

import (
	"errors"
	"fmt"
	"github.com/bluesky-social/jetstream/internal/hypercerts/jobs"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPolicyAdoptionReadinessResponseDoesNotMutate(t *testing.T) {
	h, manager := setup(t)
	_, err := manager.AddSource("https://pds.example")
	require.NoError(t, err)
	beforePolicy, beforeJobs := h.policy.Current(), manager.List()
	manager.AddRequirePolicyAdoption(true)
	response := request(h, http.MethodPut, "/policy", `{"expectedRevision":1,"collections":[]}`, testToken)
	require.Equal(t, http.StatusServiceUnavailable, response.Code)
	require.JSONEq(t, `{"error":"policy_adoption_not_ready"}`, response.Body.String())
	require.Equal(t, "no-store", response.Header().Get("Cache-Control"))
	require.Equal(t, beforePolicy, h.policy.Current())
	require.Equal(t, beforeJobs, manager.List())
}

func TestPolicyReadinessFailureMappingPreservesOtherErrors(t *testing.T) {
	for _, tc := range []struct {
		err    error
		status int
		code   string
	}{
		{fmt.Errorf("wrapped: %w", jobs.ErrPolicyAdoptionNotReady), 503, "policy_adoption_not_ready"},
		{jobs.ErrNotFound, 404, "not_found"},
		{jobs.ErrConflict, 409, "state_conflict"},
		{selection.ErrRevision, 409, "state_conflict"},
		{jobs.ErrInvalidInput, 400, "invalid_input"},
		{errors.New("private storage failure"), 500, "persistence_error"},
	} {
		response := httptest.NewRecorder()
		failure(response, tc.err)
		require.Equal(t, tc.status, response.Code)
		require.JSONEq(t, fmt.Sprintf(`{"error":%q}`, tc.code), response.Body.String())
	}
}
