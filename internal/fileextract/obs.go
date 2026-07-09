package fileextract

// obs.go — file-extractor 的最小可观测 HTTP 端点（/healthz /readyz /metrics）。
// 照搬 sibling internal/consumer/obs.go 的形态：liveness / readiness / Prometheus /metrics
// （走 counters 的私有 registry）。metrics 为 nil 时（idle 停用态）暴露空 /metrics，
// 保证探针拿 200 且无 series。

import (
	"context"
	"errors"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// writeBody 写响应体，写失败只 log 不 fail（health/metrics 端点的断连不可处理）。
func writeBody(w http.ResponseWriter, body string) {
	if _, err := w.Write([]byte(body)); err != nil {
		log.Printf("file-extractor: obs write: %v", err)
	}
}

// ObsServer 提供 /healthz（liveness）/readyz（readiness）/metrics（Prometheus）。
type ObsServer struct {
	srv   *http.Server
	ready atomic.Bool
}

// ReadyCheck 报告依赖是否可达（供 /readyz 用）。
type ReadyCheck func(ctx context.Context) error

// NewObsServer 构造绑定到 addr 的可观测 server。metrics 为 nil 时 /metrics 空转。
func NewObsServer(addr string, metrics *counters, ready ReadyCheck) *ObsServer {
	o := &ObsServer{}
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
	// /metrics 走 client_golang handler + counters 私有 registry；metrics 为 nil 时
	// 暴露空 handler（idle 态探针仍拿 200，无 series）。
	if metrics != nil {
		mux.Handle("/metrics", promhttp.HandlerFor(metrics.Registry(), promhttp.HandlerOpts{}))
	} else {
		mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		})
	}
	o.srv = &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return o
}

// SetReady 翻转就绪标志（运行循环启动后调一次）。
func (o *ObsServer) SetReady(v bool) { o.ready.Store(v) }

// Start 后台起 HTTP server；非优雅关闭的错误经 logf 上报。
func (o *ObsServer) Start(logf func(string, ...any)) {
	go func() {
		if err := o.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logf("file-extractor: obs server error: %v", err)
		}
	}()
}

// Shutdown 优雅停止 HTTP server。
func (o *ObsServer) Shutdown(ctx context.Context) error {
	return o.srv.Shutdown(ctx)
}
