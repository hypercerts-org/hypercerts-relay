package main

// hypercerts: Private service contract for the separate OAuth control plane.
import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/cmd/relay/relay"
	"github.com/bluesky-social/indigo/cmd/relay/relay/models"
)

func (s *Service) controlHandler(token string) (http.Handler, error) {
	if len(token) < 32 {
		return nil, errors.New("control token must contain at least 32 bytes")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /hypercerts/v1/sources", func(w http.ResponseWriter, r *http.Request) {
		after, err := strconv.ParseUint(r.URL.Query().Get("after"), 10, 64)
		if err != nil && r.URL.Query().Get("after") != "" {
			controlReply(w, 400, map[string]string{"error": "invalid_cursor"})
			return
		}
		page, err := s.relay.ListSources(r.Context(), after, 100)
		controlResult(w, page, err)
	})
	mux.HandleFunc("GET /hypercerts/v1/source", func(w http.ResponseWriter, r *http.Request) {
		view, err := s.relay.InspectSource(r.Context(), r.URL.Query().Get("pds"))
		controlResult(w, view, err)
	})
	mux.HandleFunc("PUT /hypercerts/v1/source", func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			PDS   string             `json:"pds"`
			State models.SourceState `json:"state"`
		}
		if !controlDecode(w, r, &input) {
			return
		}
		if input.State != models.SourceStateEnabled && input.State != models.SourceStateDisabled && input.State != models.SourceStateRemoved {
			controlReply(w, 400, map[string]string{"error": "invalid_source_state"})
			return
		}
		var view *relay.SourceView
		var err error
		if input.State == models.SourceStateEnabled {
			view, err = s.relay.AddSource(r.Context(), input.PDS)
			if err == nil && view.Validation.Status != models.SourceValidationPassed {
				view, err = s.relay.ValidateSource(r.Context(), view.HostID, view.Revision)
			}
		} else {
			view, err = s.relay.InspectSource(r.Context(), input.PDS)
		}
		if err == nil {
			view, err = s.relay.SetSourceState(r.Context(), view.HostID, view.Revision, input.State)
		}
		controlResult(w, view, err)
	})
	mux.HandleFunc("GET /hypercerts/v1/limits", func(w http.ResponseWriter, r *http.Request) {
		rows, next, err := s.relay.ListRatePolicies(r.Context(), r.URL.Query().Get("after"))
		controlResult(w, map[string]any{"items": rows, "next": next}, err)
	})
	mux.HandleFunc("PUT /hypercerts/v1/limits", func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			Scope           string `json:"scope"`
			EventsPerSecond int64  `json:"eventsPerSecond"`
		}
		if !controlDecode(w, r, &input) {
			return
		}
		p, err := s.relay.SetRatePolicy(r.Context(), input.Scope, input.EventsPerSecond)
		controlResult(w, p, err)
	})
	expected := sha256.Sum256([]byte(token))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		supplied := sha256.Sum256([]byte(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")))
		if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") || subtle.ConstantTimeCompare(expected[:], supplied[:]) != 1 {
			controlReply(w, 401, map[string]string{"error": "unauthorized"})
			return
		}
		mux.ServeHTTP(w, r)
	}), nil
}
func controlReply(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func controlResult(w http.ResponseWriter, value any, err error) {
	if err == nil {
		controlReply(w, 200, value)
		return
	}
	status, code := 500, "service_error"
	switch {
	case errors.Is(err, relay.ErrInvalidRatePolicy):
		status, code = 400, "events_per_second_must_be_1_to_1000000"
	case errors.Is(err, relay.ErrSourceNotFound):
		status, code = 404, "source_not_found"
	case errors.Is(err, relay.ErrSourceRevisionConflict):
		status, code = 409, "revision_conflict"
	case errors.Is(err, relay.ErrInvalidSourceURL), errors.Is(err, relay.ErrInvalidSourceState):
		status, code = 400, "invalid_source"
	case errors.Is(err, relay.ErrSourceValidationFailed), errors.Is(err, relay.ErrSourceDomainBanned):
		status, code = 422, "source_validation_failed"
	}
	controlReply(w, status, map[string]any{"error": code, "observed": value})
}
func controlDecode(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		controlReply(w, 400, map[string]string{"error": "invalid_json"})
		return false
	}
	if err := dec.Decode(new(any)); !errors.Is(err, io.EOF) {
		controlReply(w, 400, map[string]string{"error": "invalid_json"})
		return false
	}
	return true
}
func (s *Service) startControl(ctx context.Context, addr, tokenFile string) (func(), error) {
	if addr == "" && tokenFile == "" {
		return func() {}, nil
	}
	if addr == "" || tokenFile == "" {
		return nil, errors.New("RELAY_CONTROL_ADDR and RELAY_CONTROL_TOKEN_FILE must be configured together")
	}
	secret, err := os.ReadFile(tokenFile)
	if err != nil {
		return nil, err
	}
	h, err := s.controlHandler(strings.TrimSpace(string(secret)))
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	server := &http.Server{Handler: h, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 30 * time.Second}
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Error("private control listener stopped", "err", err)
		}
	}()
	return func() {
		shutdown, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}, nil
}
