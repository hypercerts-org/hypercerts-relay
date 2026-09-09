package relay

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"gorm.io/gorm/clause"
)

var ErrInvalidRatePolicy = errors.New("eventsPerSecond must be an integer from 1 to 1000000")

type RatePolicy struct {
	Scope           string    `gorm:"primaryKey" json:"scope"`
	EventsPerSecond int64     `json:"eventsPerSecond"`
	UpdatedAt       time.Time `json:"updatedAt"`
}

func (RatePolicy) TableName() string { return "hypercerts_rate_policy" }

type rateBucket struct {
	policy  RatePolicy
	tokens  float64
	at      time.Time
	waiting int
}
type ratePolicies struct {
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
	r.rates.mu.Lock()
	defer r.rates.mu.Unlock()
	if err := r.db.WithContext(ctx).Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "scope"}}, DoUpdates: clause.AssignmentColumns([]string{"events_per_second", "updated_at"})}).Create(&p).Error; err != nil {
		return nil, err
	}
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
	Unit               string `json:"unit"`
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
		out = append(out, RatePolicyView{RatePolicy: p, WaitingConnections: waiting, Unit: "events/second", Recovery: "Backpressure pauses socket reads; reconnect uses the last durable cursor. A replay gap requires a Jetstream recovery job; historical completeness is not guaranteed."})
	}
	return out, next, nil
}

// waitRateCapacity runs before scheduler admission, so throttling does not grow its queue.
// Both policies must allow an event. Burst capacity equals one second of its rate.
func (r *Relay) waitRateCapacity(ctx context.Context, hostname string) error {
	if r.rates == nil {
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		r.rates.mu.Lock()
		now := time.Now()
		delay := time.Duration(0)
		var buckets []*rateBucket
		for _, key := range []string{"global", "https://" + hostname, "http://" + hostname} {
			if b := r.rates.buckets[key]; b != nil {
				b.tokens = math.Min(float64(b.policy.EventsPerSecond), b.tokens+now.Sub(b.at).Seconds()*float64(b.policy.EventsPerSecond))
				b.at = now
				if b.tokens < 1 {
					delay = max(delay, time.Duration((1-b.tokens)/float64(b.policy.EventsPerSecond)*float64(time.Second)))
				}
				buckets = append(buckets, b)
			}
		}
		if delay <= 0 {
			for _, b := range buckets {
				b.tokens--
			}
			r.rates.mu.Unlock()
			return nil
		}
		for _, b := range buckets {
			b.waiting++
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
