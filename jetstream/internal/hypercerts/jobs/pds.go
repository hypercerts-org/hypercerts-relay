package jobs

import (
	"bufio"
	"context"
	"errors"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
)

type PDSProcessor struct {
	Manager    *Manager
	HTTPClient *http.Client
	Directory  *identity.Directory
	Reconcile  func(context.Context, ingest.Snapshot) error
}

const (
	pdsStageListRepos      = "listRepos"
	pdsStageGetRepoRequest = "getRepo/request"
	pdsStageGetRepoBody    = "getRepo/body"

	pdsCauseTimeout   = "timeout"
	pdsCauseCanceled  = "cancel"
	pdsCauseBodyRead  = "body_read"
	pdsCauseHTTP      = "http"
	pdsCauseTransport = "transport"
	pdsCauseUnknown   = "unknown"

	maxPDSServerRetryWait = 30 * time.Minute
)

type pdsResponseCaptureKey struct{}

type pdsResponseCapture struct {
	status int
	header http.Header
}

type pdsResponseCaptureTransport struct{ base http.RoundTripper }

func (t pdsResponseCaptureTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	response, err := base.RoundTrip(request)
	if response != nil {
		if capture, ok := request.Context().Value(pdsResponseCaptureKey{}).(*pdsResponseCapture); ok {
			capture.status = response.StatusCode
			capture.header = response.Header.Clone()
		}
	}
	return response, err
}

type pdsAttemptFailure struct {
	outcome       *InputError
	stage         string
	causeClass    string
	repositoryDID string
	httpStatus    int
	retryable     bool
	retryAt       time.Time
}

func (e *pdsAttemptFailure) Error() string { return e.outcome.Error() }

func (e *pdsAttemptFailure) Unwrap() error { return e.outcome }

type pdsJobOutcome struct {
	input   *InputError
	failure *pdsAttemptFailure
}

func (e *pdsJobOutcome) Error() string { return e.input.Error() }

func (e *pdsJobOutcome) Unwrap() []error { return []error{e.input, e.failure} }

func newPDSAttemptFailure(outcome *InputError, cause error, stage, repositoryDID string, bodyRead bool, capture *pdsResponseCapture) *pdsAttemptFailure {
	failure := &pdsAttemptFailure{outcome: outcome, stage: stage, repositoryDID: repositoryDID, causeClass: pdsCauseUnknown}
	var networkErr net.Error
	networkTimeout := errors.As(cause, &networkErr) && networkErr.Timeout()
	var responseErr *xrpc.Error
	_ = errors.As(cause, &responseErr)
	if capture != nil {
		failure.retryAt = pdsServerRetryDeadline(capture.header, time.Now())
	}
	switch {
	case errors.Is(cause, context.DeadlineExceeded) || networkTimeout:
		failure.causeClass = pdsCauseTimeout
		failure.retryable = true
	case errors.Is(cause, context.Canceled):
		failure.causeClass = pdsCauseCanceled
	case bodyRead:
		failure.causeClass = pdsCauseBodyRead
		failure.retryable = true
	case capture != nil && capture.status >= 400:
		failure.causeClass = pdsCauseHTTP
		failure.httpStatus = capture.status
		failure.retryable = retryablePDSStatus(capture.status)
	case responseErr != nil && responseErr.StatusCode > 0:
		failure.causeClass = pdsCauseHTTP
		failure.httpStatus = responseErr.StatusCode
		failure.retryable = retryablePDSStatus(responseErr.StatusCode)
	default:
		var urlErr *url.Error
		if errors.As(cause, &urlErr) || networkErr != nil {
			failure.causeClass = pdsCauseTransport
			failure.retryable = true
		}
	}
	return failure
}

func retryablePDSStatus(status int) bool {
	return status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500 && status <= 599
}

func pdsServerRetryDeadline(headers http.Header, now time.Time) time.Time {
	ceiling := now.Add(maxPDSServerRetryWait)
	latest := pdsRateLimitResetDeadline(strings.TrimSpace(headers.Get("RateLimit-Reset")), now, ceiling)
	if retryAfter := pdsRetryAfterDeadline(strings.TrimSpace(headers.Get("Retry-After")), now, ceiling); retryAfter.After(latest) {
		latest = retryAfter
	}
	return latest
}

func pdsRateLimitResetDeadline(value string, now, ceiling time.Time) time.Time {
	unix, err := strconv.ParseInt(value, 10, 64)
	if err != nil || unix <= now.Unix() {
		return time.Time{}
	}
	if unix > ceiling.Unix() {
		return ceiling
	}
	return time.Unix(unix, 0).UTC()
}

func pdsRetryAfterDeadline(value string, now, ceiling time.Time) time.Time {
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		if seconds < 0 {
			return time.Time{}
		}
		if seconds > int64(maxPDSServerRetryWait/time.Second) {
			return ceiling
		}
		return boundedPDSRetryDeadline(now.Add(time.Duration(seconds)*time.Second), now, ceiling)
	}
	parsed, err := http.ParseTime(value)
	if err != nil {
		return time.Time{}
	}
	return boundedPDSRetryDeadline(parsed.UTC(), now, ceiling)
}

func boundedPDSRetryDeadline(candidate, now, ceiling time.Time) time.Time {
	if !candidate.After(now) {
		return time.Time{}
	}
	if candidate.After(ceiling) {
		return ceiling
	}
	return candidate.UTC()
}

func repositoryRetryDeadline(attempt int, serverDeadline, now time.Time) time.Time {
	delay := repositoryRetryBaseDelay
	for step := 1; step < attempt && delay < repositoryRetryMaxDelay; step++ {
		delay = min(delay*2, repositoryRetryMaxDelay)
	}
	// ±20% jitter keeps concurrent repository retries from synchronizing.
	spread := int64(delay / 5)
	if spread > 0 {
		delay += time.Duration(rand.Int63n(spread*2+1) - spread)
	}
	delay = min(max(delay, time.Duration(1)), repositoryRetryMaxDelay)
	deadline := now.Add(delay)
	if serverDeadline.After(deadline) {
		deadline = serverDeadline
	}
	return deadline.UTC()
}

func (p PDSProcessor) Run(ctx context.Context, job Job) error {
	// A directory is required even for an empty inventory: otherwise a later
	// page could be treated as covered without any identity verification.
	if p.Directory == nil {
		return inputFailure(ctx, "identity_unavailable")
	}
	client := p.client(job)
	if !job.TotalReposKnown {
		if err := p.enumerateInventory(ctx, client, &job); err != nil {
			return err
		}
		var err error
		job, err = p.Manager.Get(job.ID)
		if err != nil {
			return err
		}
	}
	return p.processFrozenInventory(ctx, client, job)
}

// enumerateInventory performs exactly one checkpointed listRepos traversal for
// a job. It persists only validated active DID/revision coordinates; repository
// acquisition starts after the terminal page makes the denominator durable.
func (p PDSProcessor) enumerateInventory(ctx context.Context, client *atmossync.Client, job *Job) error {
	capture := &pdsResponseCapture{}
	requestCtx := context.WithValue(ctx, pdsResponseCaptureKey{}, capture)
	for page, err := range client.ListRepos(requestCtx, 100, job.Cursor) {
		if err != nil {
			return p.recordInventoryFailure(requestCtx, job, err, capture)
		}
		if err := p.checkpointInventoryPage(job, page); err != nil {
			return err
		}
		capture.status = 0
		capture.header = nil
	}
	if !job.TotalReposKnown {
		if err := p.Manager.CheckpointInventory(job.ID, job.Cursor, nil, true); err != nil {
			return err
		}
	}
	return nil
}

func (p PDSProcessor) recordInventoryFailure(ctx context.Context, job *Job, cause error, capture *pdsResponseCapture) error {
	failure := newListReposAttemptFailure(ctx, cause, capture)
	var attemptFailure *pdsAttemptFailure
	if !errors.As(failure, &attemptFailure) || attemptFailure.httpStatus != http.StatusTooManyRequests {
		return failure
	}
	deadline := repositoryRetryDeadline(1, attemptFailure.retryAt, time.Now())
	if err := p.Manager.RecordPDSCooldown(job.ID, deadline); err != nil {
		return err
	}
	return failure
}

func (p PDSProcessor) checkpointInventoryPage(job *Job, page atmossync.ListReposPage) error {
	entries := make(map[string]string)
	for _, entry := range page.Entries {
		if !entry.Active {
			continue
		}
		if err := p.validateListedSnapshot(job, entry); err != nil {
			return err
		}
		if previous, exists := entries[string(entry.DID)]; exists && previous != entry.Rev {
			return &InputError{Code: "invalid_listing"}
		}
		entries[string(entry.DID)] = entry.Rev
	}
	if err := p.Manager.CheckpointInventory(job.ID, page.NextCursor, entries, page.NextCursor == ""); err != nil {
		return err
	}
	job.Cursor = page.NextCursor
	job.EnumeratedRepos += len(entries)
	if page.NextCursor == "" {
		job.TotalReposKnown = true
	}
	return nil
}

func newListReposAttemptFailure(ctx context.Context, cause error, capture *pdsResponseCapture) error {
	outcome := inputFailure(ctx, "source_unavailable")
	var input *InputError
	if !errors.As(outcome, &input) {
		return outcome
	}
	return newPDSAttemptFailure(input, cause, pdsStageListRepos, "", false, capture)
}

type frozenRepositoryOutcome struct {
	retryPending    bool
	stop            bool
	unavailable     bool
	unavailableCode string
	permanentCode   string
	attemptFailure  *pdsAttemptFailure
}

func (p PDSProcessor) processFrozenInventory(ctx context.Context, client *atmossync.Client, job Job) error {
	entries, err := p.Manager.Inventory(job.ID)
	if err != nil {
		return err
	}
	var outcome frozenRepositoryOutcome
	for _, frozen := range entries {
		if completedRevisionAtLeast(job.CompletedRepos[frozen.DID], frozen.Revision) {
			continue
		}
		current, err := p.processFrozenRepository(ctx, client, job, frozen)
		if err != nil {
			return err
		}
		outcome = mergeFrozenRepositoryOutcome(outcome, current)
		if current.stop {
			break
		}
	}
	if outcome.retryPending {
		return errJobYield
	}
	if outcome.permanentCode != "" {
		return &InputError{Code: outcome.permanentCode}
	}
	if !outcome.unavailable {
		return nil
	}
	if outcome.unavailableCode == "" {
		outcome.unavailableCode = "repository_unavailable"
	}
	input := &InputError{Code: outcome.unavailableCode, Unavailable: true}
	if outcome.attemptFailure != nil {
		return &pdsJobOutcome{input: input, failure: outcome.attemptFailure}
	}
	return input
}

func mergeFrozenRepositoryOutcome(total, current frozenRepositoryOutcome) frozenRepositoryOutcome {
	total.retryPending = total.retryPending || current.retryPending
	total.unavailable = total.unavailable || current.unavailable
	if total.unavailableCode == "" {
		total.unavailableCode = current.unavailableCode
	}
	if total.permanentCode == "" {
		total.permanentCode = current.permanentCode
	}
	if current.attemptFailure != nil {
		total.attemptFailure = current.attemptFailure
	}
	return total
}

func (p PDSProcessor) processFrozenRepository(ctx context.Context, client *atmossync.Client, job Job, frozen ListedRepository) (frozenRepositoryOutcome, error) {
	entry := atmossync.ListReposEntry{DID: atmos.DID(frozen.DID), Rev: frozen.Revision, Active: true}
	retry, err := p.Manager.GetRepositoryRetry(job.ID, frozen.DID)
	if err != nil {
		return frozenRepositoryOutcome{}, err
	}
	outcome, handled, err := p.persistedRetryOutcome(job, entry, retry)
	if err != nil || handled {
		return outcome, err
	}
	cooldown, found, err := p.Manager.GetPDSCooldown(job.PDS)
	if err != nil {
		return frozenRepositoryOutcome{}, err
	}
	if found && cooldown.Until.After(time.Now()) {
		return frozenRepositoryOutcome{retryPending: true, stop: true}, nil
	}
	return p.processReadyRepository(ctx, client, job, entry)
}

func (p PDSProcessor) persistedRetryOutcome(job Job, entry atmossync.ListReposEntry, retry RepositoryRetry) (frozenRepositoryOutcome, bool, error) {
	switch retry.State {
	case RepositoryRetryInFlight:
		outcome, err := p.recordInterruptedRepository(job, entry, retry)
		return outcome, true, err
	case RepositoryRetryUnresolved:
		return unresolvedRepositoryOutcome(p, job, entry, retry), true, nil
	case RepositoryRetryWait:
		if retry.RetryAt.After(time.Now()) {
			return frozenRepositoryOutcome{retryPending: true}, true, nil
		}
	case RepositoryRetryReady:
	default:
		return frozenRepositoryOutcome{}, false, errors.New("invalid repository retry state")
	}
	return frozenRepositoryOutcome{}, false, nil
}

func (p PDSProcessor) recordInterruptedRepository(job Job, entry atmossync.ListReposEntry, retry RepositoryRetry) (frozenRepositoryOutcome, error) {
	var retryAt *time.Time
	if retry.Attempts < maxRepositoryAttempts {
		deadline := repositoryRetryDeadline(retry.Attempts, time.Time{}, time.Now())
		retryAt = &deadline
	}
	stored, err := p.Manager.RecordRepositoryFailure(job.ID, string(entry.DID), RepositoryRetryFailure{Category: RepositoryFailureInterrupted, Stage: RepositoryFailureGetRepoRequest}, retryAt, nil)
	if err != nil {
		return frozenRepositoryOutcome{}, err
	}
	if stored.State == RepositoryRetryWait {
		return frozenRepositoryOutcome{retryPending: true}, nil
	}
	return frozenRepositoryOutcome{unavailable: true, unavailableCode: "repository_unavailable"}, nil
}

func unresolvedRepositoryOutcome(p PDSProcessor, job Job, entry atmossync.ListReposEntry, retry RepositoryRetry) frozenRepositoryOutcome {
	if retry.Failure != nil && retry.Failure.Category == RepositoryFailureRejected {
		return frozenRepositoryOutcome{permanentCode: p.rejectionCode(job, entry)}
	}
	outcome := frozenRepositoryOutcome{unavailable: true}
	if retry.Failure != nil {
		outcome.unavailableCode = retry.Failure.Code
	}
	return outcome
}

func (p PDSProcessor) processReadyRepository(ctx context.Context, client *atmossync.Client, job Job, entry atmossync.ListReposEntry) (frozenRepositoryOutcome, error) {
	if err := p.validateListedSnapshot(&job, entry); err != nil {
		var input *InputError
		if errors.As(err, &input) && !input.Unavailable {
			return frozenRepositoryOutcome{permanentCode: input.Code}, nil
		}
		return frozenRepositoryOutcome{}, err
	}
	attempt, err := p.Manager.BeginRepositoryAttempt(job.ID, string(entry.DID))
	if errors.Is(err, ErrRepositoryCoolingDown) || errors.Is(err, ErrRepositoryRetryNotDue) {
		return frozenRepositoryOutcome{retryPending: true, stop: true}, nil
	}
	if err != nil {
		return frozenRepositoryOutcome{}, err
	}
	if err := p.repository(ctx, client, job, entry); err == nil {
		return frozenRepositoryOutcome{}, nil
	} else if errors.Is(ctx.Err(), context.Canceled) {
		return frozenRepositoryOutcome{}, ctx.Err()
	} else {
		var input *InputError
		if !errors.As(err, &input) {
			return frozenRepositoryOutcome{}, err
		}
		return p.recordRepositoryAttemptFailure(job, entry, attempt, err, input)
	}
}

func (p PDSProcessor) recordRepositoryAttemptFailure(job Job, entry atmossync.ListReposEntry, attempt RepositoryRetry, err error, input *InputError) (frozenRepositoryOutcome, error) {
	var attemptFailure *pdsAttemptFailure
	_ = errors.As(err, &attemptFailure)
	permanent := !input.Unavailable
	if permanent {
		rejectErr := p.rejectSnapshot(&job, string(entry.DID), entry.Rev, input.Code)
		var rejectionInput *InputError
		if !errors.As(rejectErr, &rejectionInput) {
			return frozenRepositoryOutcome{}, rejectErr
		}
	}
	failure := repositoryFailureFor(err, permanent)
	retryAt, pdsCooldown := repositoryRetrySchedule(attempt, input, attemptFailure)
	stored, err := p.Manager.RecordRepositoryFailure(job.ID, string(entry.DID), failure, retryAt, pdsCooldown)
	if err != nil {
		return frozenRepositoryOutcome{}, err
	}
	return repositoryAttemptFailureOutcome(input, attemptFailure, permanent, stored), nil
}

func repositoryRetrySchedule(attempt RepositoryRetry, input *InputError, attemptFailure *pdsAttemptFailure) (*time.Time, *time.Time) {
	rateLimited := attemptFailure != nil && attemptFailure.httpStatus == http.StatusTooManyRequests
	canRetry := input.Unavailable && input.Code != "repository_size_limit" && (attemptFailure == nil && input.Code != "source_changed" || attemptFailure != nil && attemptFailure.retryable) && attempt.Attempts < maxRepositoryAttempts
	if !canRetry && !rateLimited {
		return nil, nil
	}
	serverDeadline := time.Time{}
	if attemptFailure != nil {
		serverDeadline = attemptFailure.retryAt
	}
	deadline := repositoryRetryDeadline(attempt.Attempts, serverDeadline, time.Now())
	var retryAt *time.Time
	if canRetry {
		retryAt = &deadline
	}
	var pdsCooldown *time.Time
	if rateLimited {
		pdsCooldown = &deadline
	}
	return retryAt, pdsCooldown
}

func repositoryAttemptFailureOutcome(input *InputError, attemptFailure *pdsAttemptFailure, permanent bool, stored RepositoryRetry) frozenRepositoryOutcome {
	outcome := frozenRepositoryOutcome{attemptFailure: attemptFailure}
	if permanent {
		outcome.permanentCode = input.Code
	} else if stored.State == RepositoryRetryWait {
		outcome.retryPending = true
	} else {
		outcome.unavailable = true
		outcome.unavailableCode = input.Code
	}
	if attemptFailure != nil && attemptFailure.httpStatus == http.StatusTooManyRequests {
		// A 429 cools the whole admitted origin; do no more PDS work in this invocation.
		outcome.retryPending = true
		outcome.stop = true
	}
	return outcome
}

func (p PDSProcessor) rejectionCode(job Job, entry atmossync.ListReposEntry) string {
	if rejection, ok := p.Manager.lookupSnapshotRejection(job.PDS, job.Policy.Revision, string(entry.DID), entry.Rev, directPDSSnapshotRejectionKind); ok {
		return rejection.Code
	}
	return "snapshot_rejected"
}

func repositoryFailureFor(err error, permanent bool) RepositoryRetryFailure {
	stage := RepositoryFailureGetRepoRequest
	code := ""
	var input *InputError
	if errors.As(err, &input) {
		code = input.Code
	}
	if code == "repository_size_limit" {
		return RepositoryRetryFailure{Category: RepositoryFailureUnknown, Stage: RepositoryFailureGetRepoBody, Code: code}
	}
	var attemptFailure *pdsAttemptFailure
	if errors.As(err, &attemptFailure) {
		if attemptFailure.stage == pdsStageGetRepoBody {
			stage = RepositoryFailureGetRepoBody
		}
		if permanent {
			return RepositoryRetryFailure{Category: RepositoryFailureRejected, Stage: stage, Code: code}
		}
		switch attemptFailure.causeClass {
		case pdsCauseTimeout:
			return RepositoryRetryFailure{Category: RepositoryFailureTimeout, Stage: stage, Code: code}
		case pdsCauseTransport:
			return RepositoryRetryFailure{Category: RepositoryFailureTransport, Stage: stage, Code: code}
		case pdsCauseBodyRead:
			return RepositoryRetryFailure{Category: RepositoryFailureBodyRead, Stage: stage, Code: code}
		case pdsCauseHTTP:
			return RepositoryRetryFailure{Category: RepositoryFailureHTTP, HTTPStatus: attemptFailure.httpStatus, Stage: stage, Code: code}
		}
	}
	if permanent {
		return RepositoryRetryFailure{Category: RepositoryFailureRejected, Stage: stage, Code: code}
	}
	return RepositoryRetryFailure{Category: RepositoryFailureUnknown, Stage: stage, Code: code}
}

func (p PDSProcessor) client(job Job) *atmossync.Client {
	// Direct snapshots stay on the explicitly admitted origin; a redirect must
	// not silently enroll a migration target or change coverage attribution.
	httpClient := *p.HTTPClient
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	baseTransport := httpClient.Transport
	if baseTransport == nil {
		baseTransport = http.DefaultTransport
	}
	// Raw response headers are needed to combine Retry-After with
	// RateLimit-Reset; xrpc.Error retains only one parsed reset value.
	httpClient.Transport = pdsResponseCaptureTransport{base: baseTransport}
	return atmossync.NewClient(atmossync.Options{Client: &xrpc.Client{Host: job.PDS, HTTPClient: gt.Some(&httpClient), Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}, Directory: gt.Some(p.Directory)})
}

func (p PDSProcessor) processPage(ctx context.Context, client *atmossync.Client, job *Job, page atmossync.ListReposPage) error {
	active, err := p.processEntries(ctx, client, job, page.Entries)
	if err != nil {
		// A permanent rejection is durable, but it is not inventory progress.
		// Leave this page's cursor and subtotal unchanged so a retry remains
		// incomplete at the rejected listing position.
		return err
	}
	return p.checkpointPage(job, page.NextCursor, active)
}

func (p PDSProcessor) processEntries(ctx context.Context, client *atmossync.Client, job *Job, entries []atmossync.ListReposEntry) (int, error) {
	active := 0
	for _, entry := range entries {
		if !entry.Active {
			continue
		}
		if !job.TotalReposKnown {
			active++
		}
		discardProgress, err := p.processActiveEntry(ctx, client, job, entry)
		if err != nil {
			if discardProgress {
				return 0, err
			}
			return active, err
		}
	}
	return active, nil
}

// processActiveEntry returns whether a transient failure must discard this
// page's uncheckpointed enumeration subtotal.
func (p PDSProcessor) processActiveEntry(ctx context.Context, client *atmossync.Client, job *Job, entry atmossync.ListReposEntry) (bool, error) {
	did := string(entry.DID)
	if err := p.validateListedSnapshot(job, entry); err != nil {
		return false, err
	}
	if rev, ok := job.CompletedRepos[did]; ok && rev == entry.Rev {
		return false, nil
	}
	if err := p.repository(ctx, client, *job, entry); err != nil {
		return p.handleRepositoryFailure(job, entry, err)
	}
	// Keep this page-local snapshot current: duplicate entries must not trigger
	// a second download before the page checkpoint commits.
	job.CompletedRepos[did] = entry.Rev
	return false, nil
}

func (p PDSProcessor) validateListedSnapshot(job *Job, entry atmossync.ListReposEntry) error {
	did := string(entry.DID)
	if _, err := atmos.ParseDID(did); err != nil {
		if rejection, ok := p.Manager.lookupSnapshotRejection(job.PDS, job.Policy.Revision, did, entry.Rev, directPDSSnapshotRejectionKind); ok {
			return &InputError{Code: rejection.Code}
		}
		return p.rejectSnapshot(job, did, entry.Rev, "invalid_listing_did")
	}
	if _, err := atmos.ParseTID(entry.Rev); err != nil {
		return p.rejectSnapshot(job, did, entry.Rev, "invalid_listing_revision")
	}
	if rejection, ok := p.Manager.lookupSnapshotRejection(job.PDS, job.Policy.Revision, did, entry.Rev, directPDSSnapshotRejectionKind); ok {
		// hypercerts: A matching durable verdict prevents another untrusted CAR
		// download, but must leave this page unacknowledged.
		return &InputError{Code: rejection.Code}
	}
	return nil
}

func (p PDSProcessor) handleRepositoryFailure(job *Job, entry atmossync.ListReposEntry, err error) (bool, error) {
	var input *InputError
	if !errors.As(err, &input) || input.Unavailable {
		return true, err
	}
	return false, p.rejectSnapshot(job, string(entry.DID), entry.Rev, input.Code)
}

// rejectSnapshot persists a permanent input verdict without acknowledging the
// listed position. Retries must stop at that position until the listing changes.
func (p PDSProcessor) rejectSnapshot(job *Job, did, listedRevision, code string) error {
	rejection, err := p.Manager.recordSnapshotRejection(job.PDS, job.Policy.Revision, did, listedRevision, directPDSSnapshotRejectionKind, code)
	if err != nil {
		return err
	}
	return &InputError{Code: rejection.Code}
}

func (p PDSProcessor) checkpointPage(job *Job, cursor string, active int) error {
	if job.TotalReposKnown {
		if err := p.Manager.Checkpoint(job.ID, "", "", cursor); err != nil {
			return err
		}
	} else {
		if err := p.Manager.CheckpointEnumeration(job.ID, cursor, active, cursor == ""); err != nil {
			return err
		}
		job.EnumeratedRepos += active
		if cursor == "" {
			job.TotalRepos = job.EnumeratedRepos
			job.TotalReposKnown = true
		}
	}
	job.Cursor = cursor
	return nil
}

func (p PDSProcessor) completeEnumeration(job Job) error {
	// Atmos does not yield an empty terminal page. It is still a complete,
	// durable inventory and therefore has a total of the saved subtotal.
	if !job.TotalReposKnown {
		return p.Manager.CheckpointEnumeration(job.ID, job.Cursor, 0, true)
	}
	return nil
}

func inputFailure(ctx context.Context, code string) error {
	if errors.Is(ctx.Err(), context.Canceled) {
		return ctx.Err()
	}
	return &InputError{Code: code, Unavailable: true}
}

func (p PDSProcessor) repository(ctx context.Context, client *atmossync.Client, job Job, entry atmossync.ListReposEntry) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	if err := p.verifySource(ctx, entry.DID, job.PDS); err != nil {
		return err
	}
	r, commit, err := fetchRepository(ctx, client, entry.DID)
	if err != nil {
		return err
	}
	if err := p.verifySnapshot(ctx, job, entry, commit); err != nil {
		return err
	}
	snapshot, err := projectSnapshot(ctx, job, entry.DID, r, commit.Rev)
	if err != nil {
		return err
	}
	if err := p.Manager.Apply(job.ID, func() error { return p.Reconcile(ctx, snapshot) }); err != nil {
		if errors.Is(err, ingest.ErrAccountUnavailable) {
			return &InputError{Code: "account_unavailable", Unavailable: true}
		}
		return err
	}
	return p.Manager.CheckpointRepository(job.ID, string(entry.DID), entry.Rev, commit.Rev, job.Cursor)
}

// repositoryReadErrors records a non-EOF getRepo body failure so it cannot
// be misclassified as a permanent CAR syntax error by the decoder above it.
type repositoryReadErrors struct {
	io.Reader
	err error
}

func (r *repositoryReadErrors) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err != nil && !errors.Is(err, io.EOF) && r.err == nil {
		r.err = err
	}
	return n, err
}

func (p PDSProcessor) verifySource(ctx context.Context, did atmos.DID, pds string) error {
	if p.Directory == nil {
		return inputFailure(ctx, "identity_unavailable")
	}
	// A cache-only directory can still reject an already-moved source before a
	// download. It cannot accept a snapshot: verifySnapshot requires a resolver
	// and forces a refresh after the download.
	if p.Directory.Resolver == nil {
		if p.Directory.Cache == nil {
			return inputFailure(ctx, "identity_unavailable")
		}
		ident, ok := p.Directory.Cache.Get(ctx, "did:"+string(did))
		if !ok || ident == nil {
			return inputFailure(ctx, "identity_unavailable")
		}
		return verifySourceIdentity(ident, pds)
	}
	ident, err := p.Directory.LookupDID(ctx, did)
	if err != nil || ident == nil {
		return inputFailure(ctx, "identity_unavailable")
	}
	return verifySourceIdentity(ident, pds)
}

func verifySourceIdentity(ident *identity.Identity, pds string) error {
	if ident == nil {
		return &InputError{Code: "identity_unavailable", Unavailable: true}
	}
	actual, err := normalizeSource(ident.PDSEndpoint())
	if err != nil || actual != pds {
		return &InputError{Code: "source_changed", Unavailable: true}
	}
	return nil
}

func fetchRepository(ctx context.Context, client *atmossync.Client, did atmos.DID) (*repo.Repo, *repo.Commit, error) {
	capture := &pdsResponseCapture{}
	ctx = context.WithValue(ctx, pdsResponseCaptureKey{}, capture)
	body, err := client.GetRepoStream(ctx, did, "")
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			return nil, nil, ctx.Err()
		}
		input := &InputError{Code: "repository_unavailable", Unavailable: true}
		return nil, nil, newPDSAttemptFailure(input, err, pdsStageGetRepoRequest, string(did), false, capture)
	}
	defer body.Close()
	// Bound transient full-CAR input. Exceeding the bound is explicit incomplete
	// coverage; unrelated CAR blocks are never written to Jetstream segments.
	limited := &io.LimitedReader{R: body, N: 64 << 20}
	readErrors := &repositoryReadErrors{Reader: limited}
	// hypercerts: A direct getRepo response is a full snapshot. Reject a CAR
	// that parses at a block boundary but omits reachable blocks as unavailable
	// rather than materializing a partial repository.
	r, commit, err := repo.LoadCompleteFromCAR(bufio.NewReader(readErrors))
	if limited.N == 0 {
		return nil, nil, &InputError{Code: "repository_size_limit", Unavailable: true}
	}
	if ctx.Err() != nil || readErrors.err != nil {
		cause := readErrors.err
		if contextCause := context.Cause(ctx); contextCause != nil {
			cause = contextCause
		}
		if errors.Is(ctx.Err(), context.Canceled) {
			return nil, nil, ctx.Err()
		}
		input := &InputError{Code: "repository_unavailable", Unavailable: true}
		return nil, nil, newPDSAttemptFailure(input, cause, pdsStageGetRepoBody, string(did), readErrors.err != nil, capture)
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		input := &InputError{Code: "repository_incomplete", Unavailable: true}
		return nil, nil, newPDSAttemptFailure(input, err, pdsStageGetRepoBody, string(did), true, capture)
	}
	if err != nil {
		return nil, nil, &InputError{Code: "invalid_repository"}
	}
	return r, commit, nil
}

func (p PDSProcessor) verifySnapshot(ctx context.Context, job Job, entry atmossync.ListReposEntry, commit *repo.Commit) error {
	if p.Directory == nil || p.Directory.Resolver == nil {
		return inputFailure(ctx, "identity_unavailable")
	}
	if commit.DID != string(entry.DID) {
		return &InputError{Code: "repository_did_mismatch"}
	}
	listedTID, err := atmos.ParseTID(entry.Rev)
	if err != nil {
		return &InputError{Code: "invalid_listing_revision"}
	}
	commitTID, err := atmos.ParseTID(commit.Rev)
	if err != nil || commitTID.Time().After(time.Now().Add(5*time.Minute)) {
		return &InputError{Code: "invalid_revision"}
	}
	// Resolve this snapshot independently instead of purging the shared
	// directory cache used by live consumers. Direct PDS jobs still require a
	// fresh binding, but their verification must not evict another request's
	// identity entry.
	refreshed, err := p.freshSnapshotIdentity(ctx, entry.DID)
	if err != nil || refreshed == nil {
		return inputFailure(ctx, "identity_unavailable")
	}
	if err := verifySourceIdentity(refreshed, job.PDS); err != nil {
		return err
	}
	key, err := refreshed.PublicKey()
	if err != nil {
		return inputFailure(ctx, "identity_unavailable")
	}
	if err := commit.VerifySignature(key); err != nil {
		return &InputError{Code: "verification_failed"}
	}
	if commitTID.Integer() < listedTID.Integer() {
		return &InputError{Code: "snapshot_behind_listing", Unavailable: true}
	}
	return nil
}

func (p PDSProcessor) freshSnapshotIdentity(ctx context.Context, did atmos.DID) (*identity.Identity, error) {
	document, err := p.Directory.Resolver.ResolveDID(ctx, did)
	if err != nil {
		return nil, err
	}
	return identity.IdentityFromDocument(document)
}

func projectSnapshot(ctx context.Context, job Job, did atmos.DID, r *repo.Repo, rev string) (ingest.Snapshot, error) {
	snapshot := ingest.Snapshot{DID: string(did), Rev: rev, Collections: job.Policy.Collections}
	err := r.Tree.Walk(func(key string, cid cbor.CID) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		collection, rkey := repo.SplitMSTKey(key)
		if !slices.Contains(job.Policy.Collections, collection) {
			return nil
		}
		payload, err := r.Store.GetBlock(cid)
		if err != nil {
			return err
		}
		event := segment.Event{Kind: segment.KindCreateResync, DID: string(did), Rev: rev, Collection: collection, Rkey: rkey, Payload: payload, WitnessedAt: time.Now().UnixMicro()}
		if err := segment.ValidateEvent(event); err != nil {
			return err
		}
		snapshot.Records = append(snapshot.Records, event)
		return nil
	})
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return ingest.Snapshot{}, err
		}
		return ingest.Snapshot{}, &InputError{Code: "unrepresentable_snapshot", Unavailable: true}
	}
	return snapshot, nil
}
