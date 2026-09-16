package relay

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrInvalidRatePolicy = errors.New("eventsPerSecond must be an integer from 1 to 1000000")

// ErrRateAdmissionHeld means another Relay process is currently responsible for
// process-local rate admission against this database. D04 deliberately does not
// define distributed rate coordination, so starting a second active reader would
// make the configured global policy untruthful.
var ErrRateAdmissionHeld = errors.New("rate admission is already held by another relay process")

type RatePolicy struct {
	Scope           string    `gorm:"primaryKey" json:"scope"`
	EventsPerSecond int64     `json:"eventsPerSecond"`
	UpdatedAt       time.Time `json:"updatedAt"`
}

func (RatePolicy) TableName() string { return "hypercerts_rate_policy" }

const rateAdmissionName = "raw-frame-rate-admission"

// rateAdmission is a short, renewable singleton lease. It is separate from
// policy state: policies persist across restart, while token balances and the
// active process identity intentionally do not.
type rateAdmission struct {
	Name      string    `gorm:"primaryKey"`
	Holder    string    `gorm:"not null"`
	ExpiresAt time.Time `gorm:"not null;index"`
}

func (rateAdmission) TableName() string { return "hypercerts_rate_admission" }

// rateAdmissionState is a process-local generation fence. Its deadline uses
// Go's monotonic clock and is set before the database lease operation, making
// it conservative even if this Relay host's wall clock differs from the
// database server.
type rateAdmissionState struct {
	mu         sync.RWMutex
	holder     string
	deadline   time.Time
	generation uint64
}

type rateBucket struct {
	policy         RatePolicy
	tokens         float64
	at             time.Time
	waiting        int
	admittedFrames uint64
	waitedFrames   uint64
}
type ratePolicies struct {
	writeMu sync.Mutex // Serialize persistence and publication without blocking admission on disk IO.
	mu      sync.Mutex
	buckets map[string]*rateBucket
	changed chan struct{}
}

func (r *Relay) loadRatePolicies() error {
	if err := r.db.AutoMigrate(&RatePolicy{}); err != nil {
		return err
	}
	var policies []RatePolicy
	if err := r.db.Find(&policies).Error; err != nil {
		return err
	}
	r.rates = &ratePolicies{buckets: map[string]*rateBucket{}, changed: make(chan struct{})}
	for _, p := range policies {
		if p.EventsPerSecond < 1 || p.EventsPerSecond > 1_000_000 {
			return ErrInvalidRatePolicy
		}
		r.rates.buckets[p.Scope] = &rateBucket{policy: p, tokens: float64(p.EventsPerSecond), at: time.Now()}
	}
	return nil
}
func (r *Relay) SetRatePolicy(ctx context.Context, scope string, value int64) (*RatePolicy, error) {
	if value < 1 || value > 1_000_000 {
		return nil, ErrInvalidRatePolicy
	}
	if scope != "global" {
		view, err := r.InspectSource(ctx, scope)
		if err != nil {
			return nil, err
		}
		scope = sourceOrigin(view)
	}
	p := RatePolicy{Scope: scope, EventsPerSecond: value, UpdatedAt: time.Now().UTC()}
	r.rates.writeMu.Lock()
	defer r.rates.writeMu.Unlock()
	if err := r.db.WithContext(ctx).Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "scope"}}, DoUpdates: clause.AssignmentColumns([]string{"events_per_second", "updated_at"})}).Create(&p).Error; err != nil {
		return nil, err
	}
	r.rates.mu.Lock()
	defer r.rates.mu.Unlock()
	bucket := r.rates.buckets[scope]
	if bucket == nil {
		bucket = &rateBucket{tokens: float64(value)}
		r.rates.buckets[scope] = bucket
	}
	// Preserve exhausted capacity when editing a policy; edits do not refill a bucket.
	bucket.tokens = math.Min(bucket.tokens, float64(value))
	bucket.policy = p
	bucket.at = time.Now()
	close(r.rates.changed)
	r.rates.changed = make(chan struct{})
	return &p, nil
}
func sourceOrigin(view *SourceView) string {
	scheme := "https://"
	if view.NoSSL {
		scheme = "http://"
	}
	return scheme + view.Hostname
}

type RatePolicyView struct {
	RatePolicy
	WaitingConnections int    `json:"waitingConnections"`
	AdmittedFrames     uint64 `json:"admittedFrames"`
	WaitedFrames       uint64 `json:"waitedFrames"`
	Unit               string `json:"unit"`
	AdmissionScope     string `json:"admissionScope"`
	MeasurementScope   string `json:"measurementScope"`
	Recovery           string `json:"recovery"`
}

func (r *Relay) ListRatePolicies(ctx context.Context, after string) ([]RatePolicyView, string, error) {
	var rows []RatePolicy
	if err := r.db.WithContext(ctx).Where("scope > ?", after).Order("scope").Limit(101).Find(&rows).Error; err != nil {
		return nil, "", err
	}
	next := ""
	if len(rows) > 100 {
		next = rows[99].Scope
		rows = rows[:100]
	}
	out := make([]RatePolicyView, 0, len(rows))
	r.rates.mu.Lock()
	defer r.rates.mu.Unlock()
	for _, p := range rows {
		waiting := 0
		if b := r.rates.buckets[p.Scope]; b != nil {
			waiting = b.waiting
		}
		var admitted, waited uint64
		if b := r.rates.buckets[p.Scope]; b != nil {
			admitted, waited = b.admittedFrames, b.waitedFrames
		}
		out = append(out, RatePolicyView{
			RatePolicy:         p,
			WaitingConnections: waiting,
			AdmittedFrames:     admitted,
			WaitedFrames:       waited,
			Unit:               "events/second",
			AdmissionScope:     "single_active_relay_process",
			MeasurementScope:   "process_local_since_start",
			Recovery:           "Backpressure pauses socket reads; reconnect uses the last durable cursor. A replay gap requires a Jetstream recovery job; historical completeness is not guaranteed.",
		})
	}
	return out, next, nil
}

// waitRateCapacity runs before scheduler admission, so throttling does not grow its queue.
// Both policies must allow an event. Burst capacity equals one second of its rate.
func (r *Relay) waitRateCapacity(ctx context.Context, hostname string) error {
	if err := r.requireRateAdmission(ctx); err != nil {
		return err
	}
	if r.rates == nil {
		return nil
	}
	waited := map[*rateBucket]struct{}{}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		r.rates.mu.Lock()
		buckets, delay := r.rates.capacity(hostname, time.Now())
		if delay <= 0 {
			// Hold the generation fence while reserving capacity. A renewal loss
			// cannot clear admission between this check and token decrement.
			releaseAdmission, err := r.lockRateAdmission()
			if err != nil {
				r.rates.mu.Unlock()
				return err
			}
			for _, b := range buckets {
				b.tokens--
				b.admittedFrames++
			}
			releaseAdmission()
			r.rates.mu.Unlock()
			return nil
		}
		for _, b := range buckets {
			b.waiting++
			if _, recorded := waited[b]; !recorded {
				b.waitedFrames++
				waited[b] = struct{}{}
			}
		}
		changed := r.rates.changed
		r.rates.mu.Unlock()
		timer := time.NewTimer(max(delay, time.Millisecond))
		select {
		case <-ctx.Done():
		case <-changed:
		case <-timer.C:
		}
		timer.Stop()
		r.rates.mu.Lock()
		for _, b := range buckets {
			b.waiting--
		}
		r.rates.mu.Unlock()
	}
}

// AcquireRateAdmission reserves the single active process permitted to enforce
// D04's process-local policy. The expiry is derived from database time so
// independently skewed Relay hosts cannot disagree on a lease boundary.
func (r *Relay) AcquireRateAdmission(ctx context.Context, holder string, ttl time.Duration) error {
	if holder == "" || ttl <= 0 {
		return ErrRateAdmissionHeld
	}
	if err := r.db.WithContext(ctx).AutoMigrate(&rateAdmission{}); err != nil {
		return err
	}
	// Take the monotonic baseline before querying the database. Starting the
	// local deadline early makes it impossible for local admission to outlive
	// the durable lease because of query or write latency.
	deadlineBase := time.Now()
	now, err := r.rateAdmissionDatabaseNow(ctx)
	if err != nil {
		return err
	}
	lease := rateAdmission{Name: rateAdmissionName, Holder: holder, ExpiresAt: now.Add(ttl)}
	result := r.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "name"}},
		DoUpdates: clause.Assignments(map[string]any{"holder": holder, "expires_at": lease.ExpiresAt}),
		Where:     clause.Where{Exprs: []clause.Expression{gorm.Expr(r.rateAdmissionExpiredSQL()+" OR holder = ?", holder)}},
	}).Create(&lease)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrRateAdmissionHeld
	}
	r.admission.mu.Lock()
	r.admission.holder = holder
	r.admission.deadline = deadlineBase.Add(ttl)
	r.admission.generation++
	r.admission.mu.Unlock()
	return nil
}

// RenewRateAdmission fails closed when this process no longer owns the lease.
func (r *Relay) RenewRateAdmission(ctx context.Context, holder string, ttl time.Duration) error {
	if holder == "" || ttl <= 0 {
		return ErrRateAdmissionHeld
	}
	deadlineBase := time.Now()
	now, err := r.rateAdmissionDatabaseNow(ctx)
	if err != nil {
		return err
	}
	expiresAt := now.Add(ttl)
	result := r.db.WithContext(ctx).Model(&rateAdmission{}).
		Where("name = ? AND holder = ? AND "+r.rateAdmissionValidSQL(), rateAdmissionName, holder).
		Update("expires_at", expiresAt)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		r.clearRateAdmission(holder)
		return ErrRateAdmissionHeld
	}
	r.admission.mu.Lock()
	if r.admission.holder == holder {
		r.admission.deadline = deadlineBase.Add(ttl)
		r.admission.generation++
	}
	r.admission.mu.Unlock()
	return nil
}

// ReleaseRateAdmission only releases the current holder's lease.
func (r *Relay) ReleaseRateAdmission(ctx context.Context, holder string) error {
	if holder == "" {
		return nil
	}
	// Fence local reads before making the durable lease available to another
	// process. This ordering prevents a graceful handoff from overlapping.
	r.clearRateAdmission(holder)
	return r.db.WithContext(ctx).Where("name = ? AND holder = ?", rateAdmissionName, holder).Delete(&rateAdmission{}).Error
}

func (r *Relay) requireRateAdmission(ctx context.Context) error {
	_ = ctx // Local frame admission intentionally avoids a per-frame DB operation.
	release, err := r.lockRateAdmission()
	if err != nil {
		return err
	}
	release()
	return nil
}

// lockRateAdmission returns a read lock that remains held through a token
// reservation. Renewal loss/release takes the write lock, so the old
// generation cannot admit a frame after it is fenced.
func (r *Relay) lockRateAdmission() (func(), error) {
	if !r.Config.RequireRateAdmission {
		return func() {}, nil
	}
	r.admission.mu.RLock()
	holder := r.admission.holder
	if holder == "" || !time.Now().Before(r.admission.deadline) {
		r.admission.mu.RUnlock()
		if holder != "" {
			r.clearRateAdmission(holder)
		}
		return nil, ErrRateAdmissionHeld
	}
	return r.admission.mu.RUnlock, nil
}

func (r *Relay) clearRateAdmission(holder string) {
	cleared := false
	r.admission.mu.Lock()
	if r.admission.holder == holder {
		r.admission.holder = ""
		r.admission.deadline = time.Time{}
		r.admission.generation++
		cleared = true
	}
	r.admission.mu.Unlock()
	if cleared && r.Slurper != nil {
		// Do not wait here: this can run from a source scheduler whose own
		// cancellation is needed to finish the subscription.
		r.Slurper.CancelSources()
	}
}

// rateAdmissionDatabaseNow returns the database server clock, not the local
// Relay clock. SQLite's julianday form preserves sub-second lease tests; the
// PostgreSQL form uses clock_timestamp rather than a transaction-start time.
func (r *Relay) rateAdmissionDatabaseNow(ctx context.Context) (time.Time, error) {
	query := "SELECT EXTRACT(EPOCH FROM clock_timestamp())"
	if r.db.Dialector.Name() == "sqlite" {
		query = "SELECT (julianday('now') - 2440587.5) * 86400.0"
	}
	var epoch float64
	if err := r.db.WithContext(ctx).Raw(query).Scan(&epoch).Error; err != nil {
		return time.Time{}, err
	}
	return time.Unix(0, int64(epoch*float64(time.Second))).UTC(), nil
}

func (r *Relay) rateAdmissionValidSQL() string {
	if r.db.Dialector.Name() == "sqlite" {
		return "julianday(expires_at) > julianday('now')"
	}
	return "expires_at > clock_timestamp()"
}

func (r *Relay) rateAdmissionExpiredSQL() string {
	if r.db.Dialector.Name() == "sqlite" {
		return "julianday(expires_at) <= julianday('now')"
	}
	return "expires_at <= clock_timestamp()"
}

// capacity refills all matching buckets under mu; callers reserve from them atomically.
func (p *ratePolicies) capacity(hostname string, now time.Time) ([]*rateBucket, time.Duration) {
	var buckets []*rateBucket
	var delay time.Duration
	for _, key := range []string{"global", "https://" + hostname, "http://" + hostname} {
		b := p.buckets[key]
		if b == nil {
			continue
		}
		rate := float64(b.policy.EventsPerSecond)
		b.tokens = math.Min(rate, b.tokens+now.Sub(b.at).Seconds()*rate)
		b.at = now
		if b.tokens < 1 {
			delay = max(delay, time.Duration((1-b.tokens)/rate*float64(time.Second)))
		}
		buckets = append(buckets, b)
	}
	return buckets, delay
}
