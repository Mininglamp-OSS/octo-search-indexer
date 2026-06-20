package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"
)

// TestServeIdleObs_HealthAndReady verifies the idle (disabled) posture serves
// both /healthz and /readyz as 200, so a deliberately-off pod does not crashloop
// under HTTP liveness/readiness probes.
func TestServeIdleObs_HealthAndReady(t *testing.T) {
	// Grab a free localhost port.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	if cerr := ln.Close(); cerr != nil {
		t.Fatalf("close probe listener: %v", cerr)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveIdleObs(ctx, addr)

	base := fmt.Sprintf("http://%s", addr)
	deadline := time.Now().Add(3 * time.Second)
	for _, path := range []string{"/healthz", "/readyz"} {
		var got int
		for time.Now().Before(deadline) {
			resp, gerr := http.Get(base + path) //nolint:noctx // short-lived test probe
			if gerr != nil {
				time.Sleep(20 * time.Millisecond)
				continue
			}
			got = resp.StatusCode
			if cerr := resp.Body.Close(); cerr != nil {
				t.Fatalf("close body: %v", cerr)
			}
			break
		}
		if got != http.StatusOK {
			t.Fatalf("idle %s = %d, want 200", path, got)
		}
	}
}
