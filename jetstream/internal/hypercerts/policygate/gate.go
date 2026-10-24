// Package policygate serializes selected-stream admission with policy adoption.
package policygate

import (
	"context"
	"errors"

	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
)

var ErrDomainRequired = errors.New("policy gate domain is required")

// Drainer exposes the durable stream boundary needed before adopting a policy.
type Drainer interface {
	DrainDurability(context.Context) error
	NextSeq() uint64
}

type Gate struct {
	token   chan struct{}
	policy  *selection.Manager
	drainer Drainer
	domain  string
}

// New binds a policy manager and durability source to one non-empty domain.
func New(policyManager *selection.Manager, drainer Drainer, domain string) *Gate {
	return &Gate{token: make(chan struct{}, 1), policy: policyManager, drainer: drainer, domain: domain}
}

type admissionKey struct{}

// WithAdmission attaches the already-admitted policy to a callback context.
func WithAdmission(ctx context.Context, policy selection.Policy) context.Context {
	return context.WithValue(ctx, admissionKey{}, policy)
}

// Admission returns a policy lease previously attached by an Admit callback.
func Admission(ctx context.Context) (selection.Policy, bool) {
	policy, ok := ctx.Value(admissionKey{}).(selection.Policy)
	return policy, ok
}

// Admit serializes admission with adoption. The callback must not call Adopt or
// Admit on this gate; callbacks execute while the gate is exclusively held.
func (g *Gate) Admit(ctx context.Context, callback func(selection.Policy) error) error {
	if err := g.lock(ctx); err != nil {
		return err
	}
	defer g.unlock()
	if callback == nil {
		return nil
	}
	if g.policy == nil {
		return errors.New("policy gate requires collection policy")
	}
	return callback(g.policy.Current())
}

// Adopt drains durability and then invokes callback with the captured boundary
// while holding the gate. The callback must not call Adopt or Admit on this gate.
func (g *Gate) Adopt(ctx context.Context, domain string, callback func(*uint64) error) error {
	if err := g.lock(ctx); err != nil {
		return err
	}
	defer g.unlock()
	if domain == "" || domain != g.domain {
		return ErrDomainRequired
	}
	if err := g.drainer.DrainDurability(ctx); err != nil {
		return err
	}
	var boundary *uint64
	if next := g.drainer.NextSeq(); next > 1 {
		value := next - 1
		boundary = &value
	}
	if callback != nil {
		return callback(boundary)
	}
	return nil
}

func (g *Gate) lock(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case g.token <- struct{}{}:
	}
	if err := ctx.Err(); err != nil {
		g.unlock()
		return err
	}
	return nil
}

func (g *Gate) unlock() { <-g.token }
