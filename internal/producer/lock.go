package producer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	liboredis "github.com/Mininglamp-OSS/octo-lib/pkg/redis"
	rd "github.com/go-redis/redis"
)

// runLockKey is the distributed mutex key for the producer's incremental
// extraction. Distinct from any other ETL lock — each cursor runs under its own.
const runLockKey = "searchetl:etl:run"

// runLockTTL is the lock lease. We renew periodically during a run to cover work
// longer than one TTL; a process crash releases via TTL expiry.
const runLockTTL = 30 * time.Minute

// luaReleaseLock is a CAS-DEL: release only when the token matches (avoids
// deleting a successor owner's lock after a lease boundary).
var luaReleaseLock = rd.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
else
  return 0
end
`)

// luaRenewLock renews only when the token matches the current owner, so we never
// extend a successor's lock.
var luaRenewLock = rd.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
  return 0
end
`)

// RunLock abstracts the Redis distributed mutex (implemented by *RedisLock).
// Abstracted so the orchestrator can be tested with a fake lock that exercises
// the three hard semantics: held across the whole tick, renew-failure aborts the
// in-flight batch, lost-lock does not double-count.
type RunLock interface {
	// Acquire grabs the lock: (true,nil)=acquired, (false,nil)=held by another,
	// (_,err)=Redis failure.
	Acquire(token string) (bool, error)
	// Renew renews: true=still held & extended, false=expired or owner changed.
	Renew(token string) (bool, error)
	// Release CAS-DELs (only when the token matches).
	Release(token string) error
}

// RedisLock is a Redis SET NX EX + Lua CAS-DEL/CAS-PEXPIRE mutex. It is the slim,
// self-contained ~10-line Redis attach the plan calls for: it uses go-redis
// directly + octo-lib BuildTLSConfig, and does NOT depend on octo-server/pkg/redis.
type RedisLock struct {
	client *rd.Client
}

// NewRedisLock builds a RedisLock from config (with explicit pool + timeouts and
// optional TLS via octo-lib BuildTLSConfig). No bare driver defaults.
func NewRedisLock(cfg Config) (*RedisLock, error) {
	opts := &rd.Options{
		Addr:         cfg.RedisAddr,
		Password:     cfg.RedisPassword,
		MaxRetries:   3,
		PoolSize:     8,
		DialTimeout:  3 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	}
	if cfg.RedisTLS {
		tlsCfg, err := liboredis.BuildTLSConfig(cfg.RedisTLSInsecureSkipVerify, cfg.RedisTLSCAFile)
		if err != nil {
			return nil, fmt.Errorf("producer: build redis tls config: %w", err)
		}
		opts.TLSConfig = tlsCfg
	}
	return &RedisLock{client: rd.NewClient(opts)}, nil
}

// Ping verifies connectivity (used by readyz).
func (l *RedisLock) Ping() error {
	return l.client.Ping().Err()
}

// Acquire grabs the lock atomically with SET NX EX.
func (l *RedisLock) Acquire(token string) (bool, error) {
	ok, err := l.client.SetNX(runLockKey, token, runLockTTL).Result()
	if err != nil {
		return false, fmt.Errorf("producer: lock acquire: %w", err)
	}
	return ok, nil
}

// Release runs the CAS-DEL (no error when the token doesn't match / expired).
func (l *RedisLock) Release(token string) error {
	_, err := luaReleaseLock.Run(l.client, []string{runLockKey}, token).Result()
	if err != nil && !errors.Is(err, rd.Nil) {
		return fmt.Errorf("producer: lock release: %w", err)
	}
	return nil
}

// Renew extends the lease while this token still holds the lock. false = lock
// expired or owner changed (the orchestrator then aborts the in-flight batch).
func (l *RedisLock) Renew(token string) (bool, error) {
	res, err := luaRenewLock.Run(l.client, []string{runLockKey}, token, runLockTTL.Milliseconds()).Result()
	if err != nil && !errors.Is(err, rd.Nil) {
		return false, fmt.Errorf("producer: lock renew: %w", err)
	}
	n, ok := res.(int64)
	return ok && n == 1, nil
}

// Close releases the underlying connection pool.
func (l *RedisLock) Close() error {
	if l.client == nil {
		return nil
	}
	return l.client.Close()
}

// lockRenewInterval is the renew period: TTL/3, leaving margin so a single Redis
// blip does not falsely trip lost-lock.
func lockRenewInterval() time.Duration {
	iv := runLockTTL / 3
	if iv < time.Second {
		iv = time.Second
	}
	return iv
}

// randomToken generates a fresh holder token per run (for CAS-DEL/CAS-PEXPIRE).
func randomToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// runLocked runs one tick while holding the Redis run-lock (cross-replica mutex).
//
// Semantics:
//   - acquire: lost the race (another replica is running) → skip this tick,
//     return nil (no error).
//   - held across the whole tick: a background goroutine renews periodically over
//     the entire "read batch → produce Kafka → advance cursor" window.
//   - 🔴 renew failure aborts immediately: Renew error or false (expired/stolen) →
//     cancel lockCtx so the in-flight batch aborts — produce + cursor advance both
//     honor lockCtx cancellation, never advancing after lock loss.
//   - release: CAS-DEL at the end (token mismatch never deletes a successor's lock).
//
// run receives lockCtx, which is canceled on lock loss.
func runLocked(ctx context.Context, lock RunLock, interval time.Duration, logf func(string, ...any), metrics *Metrics, run func(context.Context) error) error {
	token, err := randomToken()
	if err != nil {
		return err
	}
	acquired, err := lock.Acquire(token)
	if err != nil {
		// Redis failure: do not run bare (avoid multi-replica concurrency); next tick.
		logf("producer: acquire run-lock failed, skip tick: %v", err)
		return err
	}
	if !acquired {
		logf("producer: run-lock held by another replica, skip tick")
		return nil
	}

	lockCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	done := make(chan struct{})
	renewDone := make(chan struct{})
	go func() {
		defer close(renewDone)
		renewUntilDone(lock, token, interval, cancel, done, logf, metrics)
	}()

	defer func() {
		close(done) // tell the renew goroutine to stop
		<-renewDone // join: ensure no further Renew calls before we Release
		if rerr := lock.Release(token); rerr != nil {
			logf("producer: release run-lock failed: %v", rerr)
		}
	}()

	return run(lockCtx)
}

// renewUntilDone renews periodically until done closes (tick finished) or renew
// fails. On failure (err or ownership lost) it cancels lockCtx to abort the
// in-flight batch and stops renewing.
func renewUntilDone(lock RunLock, token string, interval time.Duration, cancel context.CancelFunc, done <-chan struct{}, logf func(string, ...any), metrics *Metrics) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-done:
			return
		case <-t.C:
			ok, err := lock.Renew(token)
			if err != nil {
				logf("producer: renew run-lock failed, abort in-flight tick: %v", err)
				if metrics != nil {
					metrics.MarkLockRenewFailure()
				}
				cancel()
				return
			}
			if !ok {
				logf("producer: run-lock ownership lost, abort in-flight tick")
				if metrics != nil {
					metrics.MarkLockRenewFailure()
				}
				cancel()
				return
			}
		}
	}
}
