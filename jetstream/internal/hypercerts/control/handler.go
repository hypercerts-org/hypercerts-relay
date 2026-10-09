// Package control exposes the private Jetstream service contract for the
// separate administration control plane. It owns no user login or browser UI.
package control

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bluesky-social/jetstream/internal/hypercerts/archivekeys"
	"github.com/bluesky-social/jetstream/internal/hypercerts/jobs"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
)

const Prefix = "/hypercerts/v1"

const sourcesPath = Prefix + "/sources"

const (
	maxSnapshotRejectionAfterLength = 8 << 10
	maxSnapshotRejectionPDSFilters  = 200
	maxSnapshotRejectionPDSLength   = 2048
)

type Handler struct {
	jobs        *jobs.Manager
	policy      *selection.Manager
	token       [32]byte
	mux         *http.ServeMux
	archiveKeys *archivekeys.Manager
}

// New requires a nonempty service credential. Only mount on a private listener.
func New(token string, manager *jobs.Manager, policy *selection.Manager) (*Handler, error) {
	return NewWithArchiveKeys(token, manager, policy, nil)
}

func NewWithArchiveKeys(token string, manager *jobs.Manager, policy *selection.Manager, keys *archivekeys.Manager) (*Handler, error) {
	if len(token) < 32 || strings.TrimSpace(token) != token {
		return nil, errors.New("control token must contain at least 32 bytes and no surrounding whitespace")
	}
	if manager == nil || policy == nil {
		return nil, errors.New("control interface requires managed collection policy and jobs")
	}
	h := &Handler{jobs: manager, policy: policy, archiveKeys: keys, token: sha256.Sum256([]byte(token)), mux: http.NewServeMux()}
	h.mux.HandleFunc("GET "+Prefix+"/policy", func(w http.ResponseWriter, r *http.Request) { reply(w, http.StatusOK, h.policy.Current()) })
	h.mux.HandleFunc("PUT "+Prefix+"/policy", h.setPolicy)
	h.mux.HandleFunc("GET "+sourcesPath, func(w http.ResponseWriter, r *http.Request) {
		reply(w, http.StatusOK, map[string]any{"sources": h.jobs.Sources()})
	})
	h.mux.HandleFunc("POST "+sourcesPath, h.addSource)
	h.mux.HandleFunc("DELETE "+sourcesPath, h.removeSource)
	h.mux.HandleFunc("GET "+Prefix+"/jobs", h.listJobs)
	h.mux.HandleFunc("GET "+Prefix+"/snapshot-rejections", h.listSnapshotRejections)
	h.mux.HandleFunc("GET "+Prefix+"/coverage", h.listCoverage)
	h.mux.HandleFunc("GET "+Prefix+"/jobs/{id}", h.getJob)
	h.mux.HandleFunc("GET "+Prefix+"/jobs/{id}/repositories", h.listRepositoryDetails)
	h.mux.HandleFunc("POST "+Prefix+"/jobs", h.requestJob)
	h.mux.HandleFunc("POST "+Prefix+"/jobs/{id}/cancel", h.cancelJob)
	h.mux.HandleFunc("POST "+Prefix+"/jobs/{id}/retry", h.retryJob)
	if keys != nil {
		h.mux.HandleFunc("GET "+Prefix+"/archive-keys", h.listArchiveKeys)
		h.mux.HandleFunc("POST "+Prefix+"/archive-keys", h.createArchiveKey)
		h.mux.HandleFunc("DELETE "+Prefix+"/archive-keys/{id}", h.revokeArchiveKey)
	}
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
	case errors.Is(err, jobs.ErrRepositoryDetailsSnapshotExpired):
		status = http.StatusGone
		code = "repository_snapshot_expired"
	case errors.Is(err, jobs.ErrRepositoryDetailsSnapshotTooLarge):
		status = http.StatusRequestEntityTooLarge
		code = "repository_snapshot_too_large"
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
	policy, err := h.jobs.SetPolicy(r.Context(), input.ExpectedRevision, *input.Collections)
	if err != nil {
		failure(w, err)
		return
	}
	reply(w, 200, policy)
}
func (h *Handler) addSource(w http.ResponseWriter, r *http.Request) {
	var input struct {
		PDS            string `json:"pds"`
		SourceRevision uint64 `json:"sourceRevision,omitempty"`
	}
	if !decode(w, r, &input) {
		return
	}
	j, err := h.jobs.AddSourceWithRevision(input.PDS, input.SourceRevision)
	if err != nil {
		failure(w, err)
		return
	}
	job, err := h.jobView(j)
	if err != nil {
		failure(w, err)
		return
	}
	reply(w, 202, job)
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
		PDS            string `json:"pds"`
		Reason         string `json:"reason"`
		RequestID      string `json:"requestId,omitempty"`
		SourceRevision uint64 `json:"sourceRevision,omitempty"`
	}
	if !decode(w, r, &input) {
		return
	}
	j, err := h.jobs.RequestOnceWithSourceRevision(input.PDS, input.Reason, input.RequestID, input.SourceRevision)
	if err != nil {
		failure(w, err)
		return
	}
	job, err := h.jobView(j)
	if err != nil {
		failure(w, err)
		return
	}
	reply(w, 202, job)
}
func (h *Handler) cancelJob(w http.ResponseWriter, r *http.Request) {
	if err := h.jobs.TransitionOnce(r.PathValue("id"), jobs.Canceled, r.Header.Get("Idempotency-Key")); err != nil {
		failure(w, err)
		return
	}
	h.getJob(w, r)
}
func (h *Handler) retryJob(w http.ResponseWriter, r *http.Request) {
	if err := h.jobs.TransitionOnce(r.PathValue("id"), jobs.Pending, r.Header.Get("Idempotency-Key")); err != nil {
		failure(w, err)
		return
	}
	h.getJob(w, r)
}
func (h *Handler) listRepositoryDetails(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 200 {
			reply(w, http.StatusBadRequest, map[string]string{"error": "invalid_limit"})
			return
		}
		limit = n
	}
	snapshot, err := h.jobs.RepositoryDetails(r.PathValue("id"), r.URL.Query().Get("after"), limit)
	if err != nil {
		failure(w, err)
		return
	}
	reply(w, http.StatusOK, struct {
		Job          jobView                 `json:"job"`
		Repositories []jobs.RepositoryDetail `json:"repositories"`
		NextCursor   string                  `json:"nextCursor,omitempty"`
	}{view(snapshot.Snapshot.Job, snapshot.Snapshot.CompletedRepos, snapshot.Snapshot.Diagnostics), snapshot.Repositories, snapshot.NextCursor})
}

func (h *Handler) getJob(w http.ResponseWriter, r *http.Request) {
	job, err := h.jobViewByID(r.PathValue("id"))
	if err != nil {
		failure(w, err)
		return
	}
	reply(w, 200, job)
}

func (h *Handler) jobView(j jobs.Job) (jobView, error) {
	snapshots, err := h.jobs.DiagnosticSnapshots([]string{j.ID})
	if err != nil {
		return jobView{}, err
	}
	return view(snapshots[0].Job, snapshots[0].CompletedRepos, snapshots[0].Diagnostics), nil
}

func (h *Handler) jobViewByID(id string) (jobView, error) {
	snapshots, err := h.jobs.DiagnosticSnapshots([]string{id})
	if err != nil {
		return jobView{}, err
	}
	return view(snapshots[0].Job, snapshots[0].CompletedRepos, snapshots[0].Diagnostics), nil
}
func (h *Handler) listCoverage(w http.ResponseWriter, r *http.Request) {
	options, ok := parseCoverageOptions(r)
	if !ok {
		reply(w, 400, map[string]string{"error": "invalid_limit"})
		return
	}
	items, next := pageCoverage(latestCoverageJobs(h.jobs.ListForDiagnostics(), options.requestedPDS), options.after, options.limit)
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.JobID)
	}
	snapshots, err := h.jobs.DiagnosticSnapshots(ids)
	if err != nil {
		failure(w, err)
		return
	}
	items = make([]coverageView, 0, len(snapshots))
	for _, snapshot := range snapshots {
		items = append(items, coverage(snapshot.Job, snapshot.CompletedRepos, snapshot.Diagnostics))
	}
	if options.summary {
		reply(w, 200, coverageSummaryPage(items, next))
		return
	}
	reply(w, 200, coveragePage(items, next))
}

type coverageOptions struct {
	limit        int
	requestedPDS map[string]struct{}
	after        string
	summary      bool
}

func parseCoverageOptions(r *http.Request) (coverageOptions, bool) {
	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 200 {
			return coverageOptions{}, false
		}
		limit = n
	}
	requestedPDS := map[string]struct{}{}
	for _, pds := range r.URL.Query()["pds"] {
		if pds != "" {
			requestedPDS[pds] = struct{}{}
		}
	}
	return coverageOptions{
		limit:        limit,
		requestedPDS: requestedPDS,
		after:        r.URL.Query().Get("after"),
		summary:      r.URL.Query().Get("summary") == "1",
	}, true
}

func latestCoverageJobs(jobsList []jobs.Job, requestedPDS map[string]struct{}) []jobs.Job {
	latest := map[string]jobs.Job{}
	for _, job := range jobsList {
		if len(requestedPDS) > 0 {
			if _, ok := requestedPDS[job.PDS]; !ok {
				continue
			}
		}
		previous, ok := latest[job.PDS]
		if !ok || job.CreatedAt.After(previous.CreatedAt) || (job.CreatedAt.Equal(previous.CreatedAt) && job.ID > previous.ID) {
			latest[job.PDS] = job
		}
	}
	all := make([]jobs.Job, 0, len(latest))
	for _, job := range latest {
		all = append(all, job)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].PDS < all[j].PDS })
	return all
}

func pageCoverage(jobsList []jobs.Job, after string, limit int) ([]coverageView, string) {
	out := make([]coverageView, 0, limit)
	for _, job := range jobsList {
		if job.PDS <= after {
			continue
		}
		if len(out) == limit {
			return out, out[len(out)-1].PDS
		}
		out = append(out, coverage(job, len(job.CompletedRepos), jobs.JobDiagnostics{}))
	}
	return out, ""
}

func coveragePage(items []coverageView, next string) struct {
	Items      []coverageView `json:"items"`
	NextCursor string         `json:"nextCursor,omitempty"`
} {
	return struct {
		Items      []coverageView `json:"items"`
		NextCursor string         `json:"nextCursor,omitempty"`
	}{items, next}
}

func coverageSummaryPage(items []coverageView, next string) struct {
	Items      []coverageSummaryView `json:"items"`
	NextCursor string                `json:"nextCursor,omitempty"`
} {
	summaries := make([]coverageSummaryView, 0, len(items))
	for _, item := range items {
		summaries = append(summaries, coverageSummary(item))
	}
	return struct {
		Items      []coverageSummaryView `json:"items"`
		NextCursor string                `json:"nextCursor,omitempty"`
	}{summaries, next}
}

// rejectionView deliberately contains only the bounded, non-payload fields in
// a durable snapshot rejection. It must not expose the ledger's storage keys.
type rejectionView struct {
	PDS            string    `json:"pds"`
	PolicyRevision uint64    `json:"policyRevision"`
	DID            string    `json:"did"`
	ListedRevision string    `json:"listedRevision"`
	Kind           string    `json:"kind"`
	Code           string    `json:"code"`
	RejectedAt     time.Time `json:"rejectedAt"`
}

type rejectionCursor rejectionView

func rejectionCursorFor(view rejectionView) string {
	encoded, _ := json.Marshal(rejectionCursor(view))
	return base64.RawURLEncoding.EncodeToString(encoded)
}

func parseRejectionCursor(value string) (rejectionCursor, bool) {
	if value == "" {
		return rejectionCursor{}, true
	}
	// Bound encoded input before base64 decoding can allocate its output.
	if len(value) > maxSnapshotRejectionAfterLength {
		return rejectionCursor{}, false
	}
	encoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return rejectionCursor{}, false
	}
	var cursor rejectionCursor
	if json.Unmarshal(encoded, &cursor) != nil || cursor.PDS == "" || cursor.PolicyRevision == 0 || cursor.DID == "" || cursor.ListedRevision == "" || cursor.Kind == "" || cursor.Code == "" || cursor.RejectedAt.IsZero() {
		return rejectionCursor{}, false
	}
	return cursor, true
}

func compareRejection(a, b rejectionView) int {
	for _, values := range [][2]string{{a.PDS, b.PDS}, {a.DID, b.DID}, {a.ListedRevision, b.ListedRevision}, {a.Kind, b.Kind}} {
		if comparison := strings.Compare(values[0], values[1]); comparison != 0 {
			return comparison
		}
	}
	if a.PolicyRevision < b.PolicyRevision {
		return -1
	}
	if a.PolicyRevision > b.PolicyRevision {
		return 1
	}
	return 0
}

func snapshotRejectionView(rejection jobs.SnapshotRejection) rejectionView {
	return rejectionView{PDS: rejection.PDS, PolicyRevision: rejection.PolicyRevision, DID: rejection.DID, ListedRevision: rejection.ListedRevision, Kind: rejection.Kind, Code: rejection.Code, RejectedAt: rejection.RejectedAt}
}

type snapshotRejectionListOptions struct {
	limit        int
	after        rejectionCursor
	hasAfter     bool
	requestedPDS map[string]struct{}
}

func parseSnapshotRejectionListOptions(r *http.Request) (snapshotRejectionListOptions, string) {
	query := r.URL.Query()
	limit := 100
	if raw := query.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 200 {
			return snapshotRejectionListOptions{}, "invalid_limit"
		}
		limit = n
	}
	rawAfter := query.Get("after")
	cursor, ok := parseRejectionCursor(rawAfter)
	if !ok {
		return snapshotRejectionListOptions{}, "invalid_cursor"
	}
	requestedPDS, ok := snapshotRejectionPDSFilters(query["pds"])
	if !ok {
		return snapshotRejectionListOptions{}, "invalid_pds"
	}
	return snapshotRejectionListOptions{limit: limit, after: cursor, hasAfter: rawAfter != "", requestedPDS: requestedPDS}, ""
}

func snapshotRejectionViews(rejections []jobs.SnapshotRejection, requestedPDS map[string]struct{}) []rejectionView {
	all := make([]rejectionView, 0)
	for _, rejection := range rejections {
		if len(requestedPDS) > 0 {
			if _, found := requestedPDS[rejection.PDS]; !found {
				continue
			}
		}
		all = append(all, snapshotRejectionView(rejection))
	}
	sort.Slice(all, func(i, j int) bool { return compareRejection(all[i], all[j]) < 0 })
	return all
}

func pageSnapshotRejectionViews(all []rejectionView, after rejectionCursor, hasAfter bool, limit int) ([]rejectionView, string) {
	out := make([]rejectionView, 0, limit)
	for _, rejection := range all {
		if hasAfter && compareRejection(rejection, rejectionView(after)) <= 0 {
			continue
		}
		if len(out) == limit {
			return out, rejectionCursorFor(out[len(out)-1])
		}
		out = append(out, rejection)
	}
	return out, ""
}

func (h *Handler) listSnapshotRejections(w http.ResponseWriter, r *http.Request) {
	options, errorCode := parseSnapshotRejectionListOptions(r)
	if errorCode != "" {
		reply(w, 400, map[string]string{"error": errorCode})
		return
	}
	all := snapshotRejectionViews(h.jobs.ListSnapshotRejections(), options.requestedPDS)
	out, next := pageSnapshotRejectionViews(all, options.after, options.hasAfter, options.limit)
	reply(w, 200, struct {
		Rejections []rejectionView `json:"rejections"`
		NextCursor string          `json:"nextCursor,omitempty"`
	}{out, next})
}

func snapshotRejectionPDSFilters(values []string) (map[string]struct{}, bool) {
	// Validate the repeated query values before allocating a map sized by
	// caller-controlled input.
	if len(values) > maxSnapshotRejectionPDSFilters {
		return nil, false
	}
	for _, pds := range values {
		if len(pds) > maxSnapshotRejectionPDSLength {
			return nil, false
		}
	}
	requestedPDS := make(map[string]struct{}, len(values))
	for _, pds := range values {
		if pds != "" {
			requestedPDS[pds] = struct{}{}
		}
	}
	return requestedPDS, true
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
	selected := make([]jobs.Job, 0, limit)
	next := ""
	for _, j := range h.jobs.ListForDiagnostics() {
		if pds := r.URL.Query().Get("pds"); pds != "" && j.PDS != pds {
			continue
		}
		if j.ID <= after {
			continue
		}
		if len(selected) == limit {
			next = selected[len(selected)-1].ID
			break
		}
		selected = append(selected, j)
	}
	ids := make([]string, 0, len(selected))
	for _, j := range selected {
		ids = append(ids, j.ID)
	}
	snapshots, err := h.jobs.DiagnosticSnapshots(ids)
	if err != nil {
		failure(w, err)
		return
	}
	out := make([]jobView, 0, len(snapshots))
	for _, snapshot := range snapshots {
		out = append(out, view(snapshot.Job, snapshot.CompletedRepos, snapshot.Diagnostics))
	}
	reply(w, 200, struct {
		Jobs       []jobView `json:"jobs"`
		NextCursor string    `json:"nextCursor,omitempty"`
	}{out, next})
}

// jobView reports bounded progress counters instead of embedding every DID in
// each response. Current-state coverage and historical coverage stay separate.
type jobView struct {
	ID              string              `json:"id"`
	PDS             string              `json:"pds"`
	Policy          selection.Policy    `json:"policy"`
	Reason          string              `json:"reason"`
	State           jobs.State          `json:"state"`
	Attempts        int                 `json:"attempts"`
	CompletedRepos  int                 `json:"completedRepos"`
	TotalRepos      int                 `json:"totalRepos"`
	TotalReposKnown bool                `json:"totalReposKnown"`
	Cursor          string              `json:"cursor"`
	ErrorCode       string              `json:"errorCode,omitempty"`
	CreatedAt       time.Time           `json:"createdAt"`
	StartedAt       time.Time           `json:"startedAt"`
	FinishedAt      time.Time           `json:"finishedAt"`
	Coverage        string              `json:"coverage"`
	Diagnostics     jobs.JobDiagnostics `json:"diagnostics"`
}

func view(j jobs.Job, completedRepos int, diagnostics jobs.JobDiagnostics) jobView {
	return jobView{ID: j.ID, PDS: j.PDS, Policy: j.Policy, Reason: j.Reason, State: j.State, Attempts: j.Attempts, CompletedRepos: completedRepos, TotalRepos: j.TotalRepos, TotalReposKnown: j.TotalReposKnown, Cursor: j.Cursor, ErrorCode: j.ErrorCode, CreatedAt: j.CreatedAt, StartedAt: j.StartedAt, FinishedAt: j.FinishedAt, Coverage: j.Coverage, Diagnostics: diagnostics}
}

type coverageView struct {
	PDS             string              `json:"pds"`
	Policy          selection.Policy    `json:"policy"`
	JobID           string              `json:"jobId"`
	Reason          string              `json:"reason"`
	State           jobs.State          `json:"state"`
	CompletedRepos  int                 `json:"completedRepos"`
	TotalRepos      int                 `json:"totalRepos"`
	TotalReposKnown bool                `json:"totalReposKnown"`
	ErrorCode       string              `json:"errorCode,omitempty"`
	CreatedAt       time.Time           `json:"createdAt"`
	Coverage        string              `json:"coverage"`
	Diagnostics     jobs.JobDiagnostics `json:"diagnostics"`
}

func coverage(j jobs.Job, completedRepos int, diagnostics jobs.JobDiagnostics) coverageView {
	return coverageView{PDS: j.PDS, Policy: j.Policy, JobID: j.ID, Reason: j.Reason, State: j.State, CompletedRepos: completedRepos, TotalRepos: j.TotalRepos, TotalReposKnown: j.TotalReposKnown, ErrorCode: j.ErrorCode, CreatedAt: j.CreatedAt, Coverage: j.Coverage, Diagnostics: diagnostics}
}

// coverageSummaryView is a compact table-enrichment view. The full policy is
// intentionally omitted because it is fetched only for the selected source.
type coverageSummaryView struct {
	PDS             string              `json:"pds"`
	JobID           string              `json:"jobId"`
	State           jobs.State          `json:"state"`
	CompletedRepos  int                 `json:"completedRepos"`
	TotalRepos      int                 `json:"totalRepos"`
	TotalReposKnown bool                `json:"totalReposKnown"`
	ErrorCode       string              `json:"errorCode,omitempty"`
	CreatedAt       time.Time           `json:"createdAt"`
	Coverage        string              `json:"coverage"`
	Diagnostics     jobs.JobDiagnostics `json:"diagnostics"`
}

func coverageSummary(view coverageView) coverageSummaryView {
	return coverageSummaryView{PDS: view.PDS, JobID: view.JobID, State: view.State, CompletedRepos: view.CompletedRepos, TotalRepos: view.TotalRepos, TotalReposKnown: view.TotalReposKnown, ErrorCode: view.ErrorCode, CreatedAt: view.CreatedAt, Coverage: view.Coverage, Diagnostics: view.Diagnostics}
}

func (h *Handler) listArchiveKeys(w http.ResponseWriter, r *http.Request) {
	reply(w, http.StatusOK, map[string]any{"keys": h.archiveKeys.List()})
}
func (h *Handler) createArchiveKey(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Name                      string `json:"name"`
		Owner                     string `json:"owner"`
		RequestsPerMinute         int    `json:"requestsPerMinute"`
		ArchiveMegabytesPerMinute int    `json:"archiveMegabytesPerMinute"`
	}
	if !decode(w, r, &in) {
		return
	}
	key, token, err := h.archiveKeys.Create(in.Name, in.Owner, in.RequestsPerMinute, in.ArchiveMegabytesPerMinute)
	if err != nil {
		if errors.Is(err, archivekeys.ErrInvalid) {
			reply(w, http.StatusBadRequest, map[string]string{"error": "invalid_input"})
		} else {
			reply(w, http.StatusInternalServerError, map[string]string{"error": "persistence_error"})
		}
		return
	}
	reply(w, 201, map[string]any{"key": key, "token": token})
}
func (h *Handler) revokeArchiveKey(w http.ResponseWriter, r *http.Request) {
	if err := h.archiveKeys.Revoke(r.PathValue("id")); err != nil {
		if errors.Is(err, archivekeys.ErrNotFound) {
			reply(w, http.StatusNotFound, map[string]string{"error": "not_found"})
		} else {
			reply(w, http.StatusInternalServerError, map[string]string{"error": "persistence_error"})
		}
		return
	}
	reply(w, 204, nil)
}
