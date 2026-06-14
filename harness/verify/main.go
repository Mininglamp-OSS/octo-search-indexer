// Command verify drives end-to-end invariant checks against the local harness
// (Kafka + OpenSearch). It assumes the stack is up, the es-indexer is consuming,
// and the seed suite has been produced. It then asserts the C2/C4 + idempotency
// + IK-tokenization invariants by querying OpenSearch directly.
//
// ISOLATION: points only at the local harness endpoints (env ES / KAFKA_BROKERS).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)
	var (
		esURL    = flag.String("es", envOr("ES_URL", "http://localhost:19200"), "OpenSearch base URL")
		index    = flag.String("index", envOr("ES_INDEX", "octo-message"), "index name")
		brokers  = flag.String("brokers", envOr("KAFKA_BROKERS", "localhost:19092"), "kafka brokers (csv)")
		topic    = flag.String("topic", envOr("KAFKA_TOPIC", "octo.message.v1"), "body topic")
		dlqTopic = flag.String("dlq", envOr("KAFKA_DLQ_TOPIC", "octo.message.v1.dlq"), "DLQ topic")
		group    = flag.String("group", envOr("KAFKA_GROUP_ID", "octo-search-indexer-harness"), "consumer group id")
		dlqStart = flag.Int64("dlq-start-offset", envInt64("DLQ_START_OFFSET", -1), "only consider DLQ records at/after this offset (fence stale records from prior runs); -1 = not provided")
		waitFor  = flag.Duration("wait", 60*time.Second, "max wait for docs to appear")
	)
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *waitFor+120*time.Second)
	defer cancel()

	v := &verifier{es: strings.TrimRight(*esURL, "/"), index: *index, hc: &http.Client{Timeout: 10 * time.Second}}
	kv := newKafkaVerifier(splitCSV(*brokers))

	// 🔴 DLQ fence resolution (fail-closed). The dlq-contains-poison check must only
	// inspect records from THIS run, otherwise stale poison-pill records from a prior
	// kept-up/failed run could yield a false PASS. If no fence was provided
	// (-dlq-start-offset / DLQ_START_OFFSET unset) we resolve it safely:
	//   - DLQ currently EMPTY  → fence 0 (clean start; nothing stale to confuse us).
	//   - DLQ currently NON-EMPTY → FAIL: refuse to run an ambiguous check. The caller
	//     must capture the DLQ end offset BEFORE seeding and pass it (run.sh does this).
	fence := *dlqStart
	if fence < 0 {
		existing, err := kv.topicEndOffset(ctx, *dlqTopic)
		if err != nil {
			log.Fatalf("dlq fence: cannot read DLQ %q end offset: %v", *dlqTopic, err)
		}
		if existing > 0 {
			log.Fatalf("dlq fence: DLQ %q already has %d record(s) and no -dlq-start-offset given; "+
				"capture the DLQ end offset BEFORE seeding and pass it (run.sh does this automatically). "+
				"Refusing to run dlq-contains-poison against possibly-stale records.", *dlqTopic, existing)
		}
		fence = 0
	}

	// Refresh so seeded docs are searchable.
	if err := v.refresh(ctx); err != nil {
		log.Fatalf("refresh: %v", err)
	}

	var failures []string
	check := func(name string, err error) {
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", name, err))
			log.Printf("FAIL %s: %v", name, err)
		} else {
			log.Printf("PASS %s", name)
		}
	}

	// 🔴 Authoritative drain proof via KAFKA (not ES doc-count): the consumer group's
	// committed offset must reach the body topic's log-end on every partition. This
	// proves the poison pills were actually consumed (and committed past), so the
	// "not indexed" assertions below are sound — and it cannot be fooled by a
	// consumer that stalled before the pills or by the DLQ-spill path.
	drainErr := kv.expectGroupDrained(ctx, *topic, *group, 90*time.Second)
	check("consumer-group-drained", drainErr)

	// Positives: these poll until present, confirming healthy indexing.
	// Invariant: idempotency — duplicate message_id => exactly one doc.
	check("idempotent-dup", v.expectDocExists(ctx, "m-dup"))
	check("idempotent-count", v.expectCount(ctx, term("message_id", "m-dup"), 1))

	// Invariant: raw_excluded doc IS indexed (content null), occupies a doc.
	check("raw-excluded-indexed", v.expectDocExists(ctx, "m-signal"))

	// Invariant: IK tokenization — Chinese query term recalls the doc.
	check("ik-recall-公园", v.expectMatchAtLeast(ctx, "content", "公园", 1))
	check("ik-recall-北京", v.expectMatchAtLeast(ctx, "content", "北京", 1))
	// English still works.
	check("en-recall-pipeline", v.expectMatchAtLeast(ctx, "content", "pipeline", 1))

	// 🔴 C4 schema gate — verify BOTH directions:
	//  (1) positive: the DLQ topic actually CONTAINS the poison-pill keys (routing happened);
	//  (2) negative: those messages are NOT in the ES body index.
	// The negative assertions only stand once the group is proven drained.
	check("dlq-contains-poison", kv.expectDLQKeys(ctx, *dlqTopic, []string{"m-badschema", "m-badjson"}, fence, 60*time.Second))
	if drainErr == nil {
		check("badschema-not-indexed", v.expectCount(ctx, term("message_id", "m-badschema"), 0))
		check("badjson-not-indexed", v.expectCount(ctx, term("message_id", "m-badjson"), 0))
	} else {
		failures = append(failures, "badschema/badjson: skipped — consumer drain not proven")
		log.Printf("FAIL badschema/badjson: skipped — consumer drain not proven (%v)", drainErr)
	}

	if len(failures) > 0 {
		log.Fatalf("e2e verify FAILED (%d): %s", len(failures), strings.Join(failures, "; "))
	}
	log.Printf("e2e verify PASSED — all invariants hold")
}

type verifier struct {
	es    string
	index string
	hc    *http.Client
}

func (v *verifier) refresh(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, v.es+"/"+v.index+"/_refresh", nil)
	if err != nil {
		return err
	}
	resp, err := v.hc.Do(req)
	if err != nil {
		return err
	}
	defer closeBody(resp)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("refresh status %d", resp.StatusCode)
	}
	return nil
}

// closeBody drains+closes a response body (errcheck-friendly helper).
func closeBody(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	if cerr := resp.Body.Close(); cerr != nil {
		log.Printf("warn: close body: %v", cerr)
	}
}

// expectDocExists polls GET /<index>/_doc/<id> until found or timeout.
func (v *verifier) expectDocExists(ctx context.Context, id string) error {
	deadline := time.Now().Add(45 * time.Second)
	for {
		if rerr := v.refresh(ctx); rerr != nil {
			log.Printf("warn: refresh: %v", rerr)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.es+"/"+v.index+"/_doc/"+id, nil)
		if err != nil {
			return err
		}
		resp, err := v.hc.Do(req)
		if err == nil {
			code := resp.StatusCode
			closeBody(resp)
			if code == http.StatusOK {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("doc %q not found before deadline", id)
		}
		time.Sleep(2 * time.Second)
	}
}

func (v *verifier) expectCount(ctx context.Context, query map[string]any, want int) error {
	got, err := v.count(ctx, query)
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("count=%d want=%d", got, want)
	}
	return nil
}

func (v *verifier) expectMatchAtLeast(ctx context.Context, field, q string, atLeast int) error {
	deadline := time.Now().Add(45 * time.Second)
	for {
		if rerr := v.refresh(ctx); rerr != nil {
			log.Printf("warn: refresh: %v", rerr)
		}
		got, err := v.count(ctx, match(field, q))
		if err == nil && got >= atLeast {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("match %q on %s returned %d (<%d)", q, field, got, atLeast)
		}
		time.Sleep(2 * time.Second)
	}
}

func (v *verifier) count(ctx context.Context, query map[string]any) (int, error) {
	body, err := json.Marshal(map[string]any{"query": query})
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, v.es+"/"+v.index+"/_count", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := v.hc.Do(req)
	if err != nil {
		return 0, err
	}
	defer closeBody(resp)
	if resp.StatusCode >= 300 {
		return 0, fmt.Errorf("_count status %d", resp.StatusCode)
	}
	var out struct {
		Count int `json:"count"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, err
	}
	return out.Count, nil
}

func term(field, val string) map[string]any {
	return map[string]any{"term": map[string]any{field: val}}
}

func match(field, q string) map[string]any {
	return map[string]any{"match": map[string]any{field: q}}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt64(k string, def int64) int64 {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}
