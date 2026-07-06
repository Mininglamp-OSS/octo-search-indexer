package fileextract

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/Mininglamp-OSS/octo-lib/contract/searchmsg"
)

// mockSource 是 messageSource 的测试假实现：预置 messages 队列，Fetch 逐个返回，
// 走到末尾返 context.DeadlineExceeded 触发 processBatch 收批（模拟 kafka 无新消息）。
type mockSource struct {
	mu       sync.Mutex
	messages []fetchedMessage
	commits  []fetchedMessage
	closed   bool
}

func (m *mockSource) Fetch(ctx context.Context) (fetchedMessage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.messages) == 0 {
		// 模拟无新消息（阻塞到 ctx 超时或取消）
		<-ctx.Done()
		return fetchedMessage{}, ctx.Err()
	}
	msg := m.messages[0]
	m.messages = m.messages[1:]
	return msg, nil
}

func (m *mockSource) Commit(ctx context.Context, msg fetchedMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.commits = append(m.commits, msg)
	return nil
}

func (m *mockSource) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

// mockDLQSink 抓 DLQ 写入。
type mockDLQSink struct {
	mu       sync.Mutex
	records  []dlqRecord
	writeErr error // 注入写失败
}

func (m *mockDLQSink) WriteDLQ(ctx context.Context, key []byte, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.writeErr != nil {
		return m.writeErr
	}
	var rec dlqRecord
	if err := json.Unmarshal(value, &rec); err != nil {
		return err
	}
	m.records = append(m.records, rec)
	return nil
}

func (m *mockDLQSink) Close() error { return nil }

// mkNonFileMessage 造一条非 file 消息（type=1 text）。
func mkNonFileMessage(t *testing.T, msgID string, contentType int) fetchedMessage {
	t.Helper()
	rawPayload := map[string]any{
		"type":    contentType,
		"content": "hello",
	}
	rawBytes, err := json.Marshal(rawPayload)
	if err != nil {
		t.Fatalf("marshal rawPayload: %v", err)
	}
	msg := searchmsg.Message{
		SchemaVersion: searchmsg.SchemaVersion,
		MessageID:     msgID,
		RawPayload:    rawBytes,
	}
	value, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal msg: %v", err)
	}
	return fetchedMessage{Topic: "test-topic", Partition: 0, Offset: 100, Key: []byte(msgID), Value: value}
}

// TestProcessOne_SkipNonFileMessages 非 file 类型（text/image/video）应跳过 + 不投 DLQ + skippedNonFile 计数 +1。
func TestProcessOne_SkipNonFileMessages(t *testing.T) {
	for _, ct := range []int{1, 2, 5, 11, 14} { // text/image/video/mergeForward/richText
		src := &mockSource{}
		dlq := &mockDLQSink{}
		p := NewProcessor(src, dlq, nil, ServiceConfig{}) // extractor 不会被走到（非 file 走 skip 分支）
		msg := mkNonFileMessage(t, "42", ct)
		if err := p.processOne(context.Background(), msg); err != nil {
			t.Fatalf("processOne ct=%d: %v", ct, err)
		}
		if len(dlq.records) != 0 {
			t.Errorf("ct=%d: expected 0 DLQ records, got %d: %+v", ct, len(dlq.records), dlq.records)
		}
		if p.metrics.skippedNonFile.Load() != 1 {
			t.Errorf("ct=%d: skippedNonFile expected 1, got %d", ct, p.metrics.skippedNonFile.Load())
		}
	}
}

// TestProcessOne_ParseErrorToDLQ 塞坏 JSON → parse_error DLQ 触发。
func TestProcessOne_ParseErrorToDLQ(t *testing.T) {
	src := &mockSource{}
	dlq := &mockDLQSink{}
	p := NewProcessor(src, dlq, nil, ServiceConfig{}) // parse_error 分支不走到 extractor
	msg := fetchedMessage{
		Topic:     "test-topic",
		Partition: 0,
		Offset:    100,
		Key:       []byte("bad-msg"),
		Value:     []byte("this is not json"),
	}
	if err := p.processOne(context.Background(), msg); err != nil {
		t.Fatalf("processOne: %v", err)
	}
	if len(dlq.records) != 1 {
		t.Fatalf("expected 1 DLQ record, got %d", len(dlq.records))
	}
	if dlq.records[0].Reason != ReasonParseError {
		t.Errorf("reason: got %q want %q", dlq.records[0].Reason, ReasonParseError)
	}
	if p.metrics.dlqTotal.Load() != 1 {
		t.Errorf("dlqTotal expected 1, got %d", p.metrics.dlqTotal.Load())
	}
}

// TestProcessOne_DLQWriteFailure_ReturnsError DLQ 自身写失败时上抛，暂停批推进（避免静默丢毒丸）。
func TestProcessOne_DLQWriteFailure_ReturnsError(t *testing.T) {
	src := &mockSource{}
	dlq := &mockDLQSink{writeErr: errors.New("kafka down")}
	p := NewProcessor(src, dlq, nil, ServiceConfig{}) // parse_error 分支不走到 extractor
	msg := fetchedMessage{Key: []byte("k"), Value: []byte("bad json")}
	err := p.processOne(context.Background(), msg)
	if err == nil {
		t.Fatal("expected error when DLQ write fails")
	}
}

// TestExtractContentTypeFile_HappyPath 覆盖 payload.go 核心解析路径。
func TestExtractContentTypeFile_HappyPath(t *testing.T) {
	raw := []byte(`{"type":8,"url":"https://cdn.deepminer.com.cn/y.pdf","name":"y.pdf","extension":".pdf","size":2048}`)
	fp, ok := extractContentTypeFile(raw)
	if !ok {
		t.Fatal("expected true for type=8")
	}
	if fp.URL != "https://cdn.deepminer.com.cn/y.pdf" || fp.Extension != ".pdf" || fp.Size != 2048 {
		t.Errorf("unexpected payload: %+v", fp)
	}
}

// TestExtractContentTypeFile_NonFileType 各种非 8 type 应返 (nil, false)。
func TestExtractContentTypeFile_NonFileType(t *testing.T) {
	cases := []string{
		`{"type":1,"content":"hello"}`,
		`{"type":2,"url":"x"}`,
		`{"type":5,"url":"y"}`,
		`{"type":11}`,
		`{"type":14}`,
	}
	for _, raw := range cases {
		if _, ok := extractContentTypeFile([]byte(raw)); ok {
			t.Errorf("expected false for %s", raw)
		}
	}
}

// TestExtractContentTypeFile_MalformedPayload 空/损坏 payload 应返 (nil, false) 不 panic。
func TestExtractContentTypeFile_MalformedPayload(t *testing.T) {
	for _, raw := range [][]byte{nil, {}, []byte(`not json`), []byte(`[]`), []byte(`123`)} {
		if _, ok := extractContentTypeFile(raw); ok {
			t.Errorf("expected false for %q", raw)
		}
	}
}
