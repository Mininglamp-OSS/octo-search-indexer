package producer

import (
	"context"
	"errors"
	"log"
	"net/http"
	"sync/atomic"
	"time"
)

// writeBody writes a response body, logging (but not failing on) a write error —
// a broken client connection on a health/metrics endpoint is not actionable.
func writeBody(w http.ResponseWriter, body string) {
	if _, err := w.Write([]byte(body)); err != nil {
		log.Printf("producer: obs write: %v", err)
	}
}

// ObsServer serves the minimal observability endpoints: /healthz (liveness),
// /readyz (readiness — backends reachable), /metrics (Prometheus text). It uses
// only net/http so the slim image gains no new dependency.
type ObsServer struct {
	srv     *http.Server
	metrics *Metrics
	ready   atomic.Bool
}

// ReadyCheck reports whether dependencies are reachable (used by /readyz).
type ReadyCheck func(ctx context.Context) error

// NewObsServer builds the observability server bound to addr.
func NewObsServer(addr string, metrics *Metrics, ready ReadyCheck) *ObsServer {
	o := &ObsServer{metrics: metrics}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		writeBody(w, "ok\n")
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if !o.ready.Load() {
			http.Error(w, "starting\n", http.StatusServiceUnavailable)
			return
		}
		if ready != nil {
			ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
			defer cancel()
			if err := ready(ctx); err != nil {
				http.Error(w, "not ready: "+err.Error()+"\n", http.StatusServiceUnavailable)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		writeBody(w, "ready\n")
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		if metrics != nil {
			writeBody(w, metrics.Render())
		}
	})
	o.srv = &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return o
}

// SetReady flips the readiness flag (call once the run loop is live).
func (o *ObsServer) SetReady(v bool) { o.ready.Store(v) }

// Start runs the HTTP server in the background; errors (other than graceful
// close) are reported via logf.
func (o *ObsServer) Start(logf func(string, ...any)) {
	go func() {
		if err := o.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logf("producer: obs server error: %v", err)
		}
	}()
}

// Shutdown gracefully stops the HTTP server.
func (o *ObsServer) Shutdown(ctx context.Context) error {
	return o.srv.Shutdown(ctx)
}
