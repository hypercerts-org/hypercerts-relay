// Package control exposes the private Jetstream service contract for the
// separate administration control plane. It owns no user login or browser UI.
package control

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/bluesky-social/jetstream/internal/hypercerts/jobs"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
)

const Prefix = "/hypercerts/v1"

type Handler struct {
	jobs   *jobs.Manager
	policy *selection.Manager
	token  [32]byte
	mux    *http.ServeMux
}

// New requires a nonempty service credential. Only mount on a private listener.
func New(token string, manager *jobs.Manager, policy *selection.Manager) (*Handler, error) {
	if len(token) < 32 || strings.TrimSpace(token) != token {
		return nil, errors.New("control token must contain at least 32 bytes and no surrounding whitespace")
	}
	if manager == nil || policy == nil {
		return nil, errors.New("control interface requires managed collection policy and jobs")
	}
	h := &Handler{jobs: manager, policy: policy, token: sha256.Sum256([]byte(token)), mux: http.NewServeMux()}
	h.mux.HandleFunc("GET "+Prefix+"/policy", func(w http.ResponseWriter, r *http.Request) { reply(w, http.StatusOK, h.policy.Current()) })
	h.mux.HandleFunc("PUT "+Prefix+"/policy", h.setPolicy)
	h.mux.HandleFunc("GET "+Prefix+"/sources", func(w http.ResponseWriter, r *http.Request) {
		reply(w, http.StatusOK, map[string]any{"sources": h.jobs.Sources()})
	})
	h.mux.HandleFunc("POST "+Prefix+"/sources", h.addSource)
	h.mux.HandleFunc("DELETE "+Prefix+"/sources", h.removeSource)
	h.mux.HandleFunc("GET "+Prefix+"/jobs", h.listJobs)
	h.mux.HandleFunc("GET "+Prefix+"/jobs/{id}", h.getJob)
	h.mux.HandleFunc("POST "+Prefix+"/jobs", h.requestJob)
	h.mux.HandleFunc("POST "+Prefix+"/jobs/{id}/cancel", h.cancelJob)
	h.mux.HandleFunc("POST "+Prefix+"/jobs/{id}/retry", h.retryJob)
	return h, nil
}
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	value := r.Header.Get("Authorization")
	supplied := sha256.Sum256([]byte(strings.TrimPrefix(value, "Bearer ")))
	if !strings.HasPrefix(value, "Bearer ") || subtle.ConstantTimeCompare(supplied[:], h.token[:]) != 1 {
		w.Header().Set("WWW-Authenticate", "Bearer")
		reply(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}
	h.mux.ServeHTTP(w, r)
}
func reply(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if value != nil {
		_ = json.NewEncoder(w).Encode(value)
	}
}
func decode(w http.ResponseWriter, r *http.Request, value any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(value); err != nil {
		reply(w, 400, map[string]string{"error": "invalid_json"})
		return false
	}
	if err := dec.Decode(new(any)); !errors.Is(err, io.EOF) {
		reply(w, 400, map[string]string{"error": "invalid_json"})
		return false
	}
	return true
}
func failure(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	code := "persistence_error"
	switch {
	case errors.Is(err, jobs.ErrNotFound):
		status = 404
		code = "not_found"
	case errors.Is(err, jobs.ErrConflict) || errors.Is(err, selection.ErrRevision):
		status = 409
		code = "state_conflict"
	case errors.Is(err, jobs.ErrInvalidInput):
		status = 400
		code = "invalid_input"
	}
	reply(w, status, map[string]string{"error": code})
}
func (h *Handler) setPolicy(w http.ResponseWriter, r *http.Request) {
	var input struct {
		ExpectedRevision uint64    `json:"expectedRevision"`
		Collections      *[]string `json:"collections"`
	}
	if !decode(w, r, &input) {
		return
	}
	if input.Collections == nil {
		reply(w, 400, map[string]string{"error": "collections_required"})
		return
	}
	if _, err := selection.Normalize(*input.Collections); err != nil {
		reply(w, 400, map[string]string{"error": "invalid_collections"})
		return
	}
	policy, err := h.jobs.SetPolicy(input.ExpectedRevision, *input.Collections)
	if err != nil {
		failure(w, err)
		return
	}
	reply(w, 200, policy)
}
func (h *Handler) addSource(w http.ResponseWriter, r *http.Request) {
	var input struct {
		PDS string `json:"pds"`
	}
	if !decode(w, r, &input) {
		return
	}
	j, err := h.jobs.AddSource(input.PDS)
	if err != nil {
		failure(w, err)
		return
	}
	reply(w, 202, view(j))
}
func (h *Handler) removeSource(w http.ResponseWriter, r *http.Request) {
	var input struct {
		PDS string `json:"pds"`
	}
	if !decode(w, r, &input) {
		return
	}
	if err := h.jobs.RemoveSource(input.PDS); err != nil {
		failure(w, err)
		return
	}
	reply(w, 204, nil)
}
func (h *Handler) requestJob(w http.ResponseWriter, r *http.Request) {
	var input struct {
		PDS    string `json:"pds"`
		Reason string `json:"reason"`
	}
	if !decode(w, r, &input) {
		return
	}
	j, err := h.jobs.Request(input.PDS, input.Reason)
	if err != nil {
		failure(w, err)
		return
	}
	reply(w, 202, view(j))
}
func (h *Handler) cancelJob(w http.ResponseWriter, r *http.Request) {
	if err := h.jobs.Cancel(r.PathValue("id")); err != nil {
		failure(w, err)
		return
	}
	h.getJob(w, r)
}
func (h *Handler) retryJob(w http.ResponseWriter, r *http.Request) {
	if err := h.jobs.Retry(r.PathValue("id")); err != nil {
		failure(w, err)
		return
	}
	h.getJob(w, r)
}
func (h *Handler) getJob(w http.ResponseWriter, r *http.Request) {
	for _, j := range h.jobs.List() {
		if j.ID == r.PathValue("id") {
			reply(w, 200, view(j))
			return
		}
	}
	failure(w, jobs.ErrNotFound)
}
func (h *Handler) listJobs(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 200 {
			reply(w, 400, map[string]string{"error": "invalid_limit"})
			return
		}
		limit = n
	}
	after := r.URL.Query().Get("after")
	out := make([]jobView, 0, limit)
	next := ""
	for _, j := range h.jobs.List() {
		if j.ID <= after {
			continue
		}
		if len(out) == limit {
			next = out[len(out)-1].ID
			break
		}
		out = append(out, view(j))
	}
	reply(w, 200, struct {
		Jobs       []jobView `json:"jobs"`
		NextCursor string    `json:"nextCursor,omitempty"`
	}{out, next})
}

// jobView reports bounded progress counters instead of embedding every DID in
// each response. Current-state coverage and historical coverage stay separate.
type jobView struct {
	ID              string           `json:"id"`
	PDS             string           `json:"pds"`
	Policy          selection.Policy `json:"policy"`
	Reason          string           `json:"reason"`
	State           jobs.State       `json:"state"`
	Attempts        int              `json:"attempts"`
	CompletedRepos  int              `json:"completedRepos"`
	Cursor          string           `json:"cursor"`
	ErrorCode       string           `json:"errorCode,omitempty"`
	CreatedAt       time.Time        `json:"createdAt"`
	StartedAt       time.Time        `json:"startedAt"`
	FinishedAt      time.Time        `json:"finishedAt"`
	Coverage        string           `json:"coverage"`
	HistoryComplete bool             `json:"historyComplete"`
}

func view(j jobs.Job) jobView {
	return jobView{ID: j.ID, PDS: j.PDS, Policy: j.Policy, Reason: j.Reason, State: j.State, Attempts: j.Attempts, CompletedRepos: len(j.CompletedRepos), Cursor: j.Cursor, ErrorCode: j.ErrorCode, CreatedAt: j.CreatedAt, StartedAt: j.StartedAt, FinishedAt: j.FinishedAt, Coverage: j.Coverage, HistoryComplete: j.HistoryComplete}
}
