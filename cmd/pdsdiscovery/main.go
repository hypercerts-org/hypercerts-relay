// Command pdsdiscovery enables PDS sources that publish retained collections.
//
// It reads the retained collections and every known PDS source from the
// administration machine API, watches a public Bluesky Jetstream for records in
// those collections, and asks administration to enable a PDS that is not yet a
// source. After the PDS appears, it raises the account limit to match the
// reference relay's account count. Every change is an audited administration
// operation; this service never calls Relay or Jetstream directly.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/hypercerts/version"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/urfave/cli/v3"
)

func main() {
	app := &cli.Command{
		Name:    "pdsdiscovery",
		Usage:   "enable PDS sources that publish retained collections",
		Version: version.Version,
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "admin-url", Required: true, Usage: "administration origin", Sources: cli.EnvVars("DISCOVERY_ADMIN_URL")},
			&cli.StringFlag{Name: "admin-token-file", Required: true, Usage: "file holding the administration machine token", Sources: cli.EnvVars("DISCOVERY_ADMIN_TOKEN_FILE")},
			&cli.StringFlag{Name: "upstream", Value: "wss://jetstream2.us-east.bsky.network/subscribe", Usage: "Bluesky Jetstream subscribe URL", Sources: cli.EnvVars("DISCOVERY_UPSTREAM")},
			&cli.StringFlag{Name: "reference-relay", Value: "https://relay1.us-east.bsky.network", Usage: "relay queried with com.atproto.sync.getHostStatus", Sources: cli.EnvVars("DISCOVERY_REFERENCE_RELAY")},
			&cli.DurationFlag{Name: "poll-interval", Value: time.Minute, Usage: "administration state poll interval", Sources: cli.EnvVars("DISCOVERY_POLL_INTERVAL")},
			&cli.StringFlag{Name: "deny-hosts", Usage: "comma-separated hostnames or *.suffix patterns never enabled", Sources: cli.EnvVars("DISCOVERY_DENY_HOSTS")},
			&cli.BoolFlag{Name: "dry-run", Usage: "log candidates without requesting operations", Sources: cli.EnvVars("DISCOVERY_DRY_RUN")},
			&cli.IntFlag{Name: "account-threshold", Value: 100, Usage: "reference account count above which the account limit is raised", Sources: cli.EnvVars("DISCOVERY_ACCOUNT_THRESHOLD")},
			&cli.DurationFlag{Name: "replay-lookback", Value: 10 * time.Minute, Usage: "upstream replay on a cold start", Sources: cli.EnvVars("DISCOVERY_REPLAY_LOOKBACK")},
			&cli.StringFlag{Name: "metrics-addr", Usage: "listen address for /metrics and /healthz", Sources: cli.EnvVars("DISCOVERY_METRICS_ADDR")},
			&cli.StringFlag{Name: "log-level", Value: "info", Sources: cli.EnvVars("DISCOVERY_LOG_LEVEL", "LOG_LEVEL")},
		},
		Action: run,
	}
	if err := app.Run(context.Background(), os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cmd *cli.Command) error {
	var level slog.Level
	if err := level.UnmarshalText([]byte(cmd.String("log-level"))); err != nil {
		return fmt.Errorf("invalid log level: %w", err)
	}
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(log)

	secret, err := os.ReadFile(cmd.String("admin-token-file"))
	if err != nil {
		return fmt.Errorf("reading administration token: %w", err)
	}
	admin, err := NewAdminClient(cmd.String("admin-url"), strings.TrimSpace(string(secret)))
	if err != nil {
		return err
	}
	relay, err := NewRelayClient(cmd.String("reference-relay"))
	if err != nil {
		return err
	}
	if cmd.Duration("poll-interval") < 5*time.Second {
		return errors.New("poll interval must be at least 5s")
	}
	cfg := Config{
		DenyHosts:        splitList(cmd.String("deny-hosts")),
		DryRun:           cmd.Bool("dry-run"),
		AccountThreshold: int64(cmd.Int("account-threshold")),
		PendingQuotaTTL:  max(time.Hour, 10*cmd.Duration("poll-interval")),
	}
	d, err := NewDiscoverer(cfg, log, identity.DefaultDirectory(), admin, relay)
	if err != nil {
		return err
	}
	up, err := NewUpstream(cmd.String("upstream"), cmd.Duration("replay-lookback"), log, d)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Start only with a current view, so nothing is enabled against stale state.
	state, err := admin.State(ctx)
	if err != nil {
		return fmt.Errorf("initial administration state: %w", err)
	}
	d.SetState(state)
	var lastSuccess atomic.Int64
	lastSuccess.Store(time.Now().Unix())
	lastPoll.SetToCurrentTime()
	log.Info("pdsdiscovery started", "collections", len(state.Collections), "sources", len(state.Sources), "dryRun", cfg.DryRun, "denyHosts", cfg.DenyHosts, "maxAccountLimit", state.MaxAccountLimit)

	var wg sync.WaitGroup
	if addr := cmd.String("metrics-addr"); addr != "" {
		mux := http.NewServeMux()
		mux.Handle("GET /metrics", promhttp.Handler())
		stale := 5 * cmd.Duration("poll-interval")
		mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
			if time.Since(time.Unix(lastSuccess.Load(), 0)) > stale {
				http.Error(w, "administration state is stale", http.StatusServiceUnavailable)
				return
			}
			_, _ = w.Write([]byte("ok\n"))
		})
		srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		wg.Go(func() {
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("metrics listener stopped", "err", err)
			}
		})
		wg.Go(func() {
			<-ctx.Done()
			shutdown, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			_ = srv.Shutdown(shutdown)
		})
	}
	wg.Go(func() { up.Run(ctx) })
	wg.Go(func() {
		ticker := time.NewTicker(cmd.Duration("poll-interval"))
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			state, err := admin.State(ctx)
			if err != nil {
				if ctx.Err() == nil {
					pollFailures.Inc()
					log.Warn("administration state poll failed; keeping last state", "err", err)
				}
				continue
			}
			if d.SetState(state) {
				up.Reload()
			}
			lastSuccess.Store(time.Now().Unix())
			lastPoll.SetToCurrentTime()
			d.Reconcile(ctx)
		}
	})
	wg.Wait()
	log.Info("pdsdiscovery stopped")
	return nil
}

func splitList(raw string) []string {
	var out []string
	for item := range strings.SplitSeq(raw, ",") {
		if item = strings.ToLower(strings.TrimSpace(item)); item != "" {
			out = append(out, item)
		}
	}
	return out
}
