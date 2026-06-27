package consumer

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/Mininglamp-OSS/octo-lib/contract/searchmsg"
	"github.com/Mininglamp-OSS/octo-search-indexer/internal/esindex"
)

// Service 把 Kafka 消费侧 + ES 写入器 + DLQ 组装成可运行单元（cmd/es-indexer 用）。
type Service struct {
	proc    *Processor
	source  *kafkaSource
	dlqSink *kafkaDLQSink
	writer  esindex.Writer
	metrics *Metrics
}

// ServiceConfig 是 es-indexer 服务的运行配置（由 cmd 从环境装配）。
type ServiceConfig struct {
	Brokers   []string
	Topic     string
	DLQTopic  string
	GroupID   string
	BatchSize int

	ESAddresses []string
	ESIndex     string
	ESUsername  string
	ESPassword  string
	// ESTLSInsecureSkipVerify 为 true 时跳过 OpenSearch HTTPS 证书校验
	// （自签证书场景）。默认 false：使用默认 transport，强制校验，行为不变。
	ESTLSInsecureSkipVerify bool

	TransientBackoff time.Duration
	DLQMaxRetries    int
	DLQRetryBackoff  time.Duration
	DLQSpillDir      string
}

// metricsAlerter is the production alerter: it maps alert events to Prometheus
// counters and still logs every event. The Alert(event, detail) interface is
// unchanged — only the implementation moved from pure logging to logging +
// counting (no alerting/priority semantics; the counters are plain).
type metricsAlerter struct {
	metrics *Metrics
}

func (a metricsAlerter) Alert(event string, detail string) {
	switch event {
	case "bulk_batch_error":
		if a.metrics != nil {
			a.metrics.MarkBulkError()
		}
	case "dlq_hard_stop":
		// 真实硬停：仅在 routePoison 拿到 DLQ 终态逃逸 err 时触发一次（无 spill），语义精确。
		if a.metrics != nil {
			a.metrics.MarkDLQHardStop()
		}
	case "dlq_write_exhausted":
		// DLQ 写重试耗尽（无论之后硬停还是 spill 越过都会触发）——独立信号，不并入 hard_stop
		// 否则会让 hard_stop 双计（硬停时 exhausted+hard_stop 各一次）+ spill 假阳性。
		if a.metrics != nil {
			a.metrics.MarkDLQWriteExhausted()
		}
	}
	log.Printf("[ALERT] %s: %s", event, detail)
}

// NewService 装配真实组件（连 Kafka + OpenSearch）。
func NewService(cfg ServiceConfig) (*Service, error) {
	var transport http.RoundTripper
	if cfg.ESTLSInsecureSkipVerify {
		// 仅在显式开启时跳过证书校验（自签证书场景）。
		transport = esindex.InsecureSkipVerifyTransport()
	}
	writer, err := esindex.NewWriter(esindex.Config{
		Addresses: cfg.ESAddresses,
		Index:     cfg.ESIndex,
		Username:  cfg.ESUsername,
		Password:  cfg.ESPassword,
		Transport: transport,
	})
	if err != nil {
		return nil, fmt.Errorf("consumer: build ES writer: %w", err)
	}

	metrics := NewMetrics()

	source, err := newKafkaSource(KafkaSourceConfig{
		Brokers: cfg.Brokers,
		Topic:   cfg.Topic,
		GroupID: cfg.GroupID,
	}, metrics)
	if err != nil {
		return nil, err
	}

	dlqSink, err := newKafkaDLQSink(cfg.Brokers, cfg.DLQTopic, metrics)
	if err != nil {
		// 避免已建好的 source reader 泄漏（P2-4：构造器后段出错须回收前段资源）。
		if cerr := source.Close(); cerr != nil {
			log.Printf("consumer: close source after DLQ-sink init failure: %v", cerr)
		}
		return nil, err
	}

	alert := metricsAlerter{metrics: metrics}
	dlqCfg := defaultDLQConfig()
	if cfg.DLQMaxRetries > 0 {
		dlqCfg.MaxRetries = cfg.DLQMaxRetries
	}
	if cfg.DLQRetryBackoff > 0 {
		dlqCfg.RetryBackoff = cfg.DLQRetryBackoff
	}
	dlqCfg.SpillDir = cfg.DLQSpillDir
	dlq := newDLQHandler(dlqSink, alert, dlqCfg)

	proc := NewProcessor(source, writer, dlq, alert, Config{
		BatchSize:        cfg.BatchSize,
		TransientBackoff: cfg.TransientBackoff,
	}, metrics)

	return &Service{proc: proc, source: source, dlqSink: dlqSink, writer: writer, metrics: metrics}, nil
}

// Metrics exposes the service's metrics registry holder (for the obs server).
func (s *Service) Metrics() *Metrics { return s.metrics }

// Run 运行消费循环直到 ctx 取消。启动时先幂等确保目标索引存在（带 mapping/中文分词），
// 再做 mapping-compat fail-closed 断言（§6.4）。
//
// 🔴 实时写入安全闸（§3.6 语义重定义）：本 gate（LiveContractCarriesSafetyFields）仅校验
// **Kafka 契约版本 ≥ v2**（带 RawPayload 投影能力的最低契约版本，当前 searchmsg.SchemaVersion 已 ==2，
// gate 恒 true）。**方案 B 后，实时写入的 visibility fail-closed 安全保证来自消费侧 processBatch
// 预检调 searchmsg.ExtractVisibility（§3.4），不再来自「契约是否带 visibles」。** 故本 gate 不是
// visibility 安全的充分条件，仅是契约版本下限闸；安全充分性由预检负责。不 bump SchemaVersion
// （consumer 严格相等校验，bump=在飞 v2 消息全进 DLQ 风暴）。
func (s *Service) Run(ctx context.Context) error {
	if !esindex.LiveContractCarriesSafetyFields() {
		return fmt.Errorf("consumer: live ingestion refused — Kafka contract (searchmsg.SchemaVersion=%d) "+
			"is below the minimum version %d that carries RawPayload projection capability; bump octo-lib "+
			"before enabling live ingestion (visibility fail-closed safety itself comes from the consumer-side "+
			"ExtractVisibility pre-check, not from this gate)",
			searchmsg.SchemaVersion, esindex.SafetyFieldsSchemaVersion)
	}
	// 先校验目标索引已存在，缺失则拒启动（不再自动创建，避免裸建丢 ISM/shards/replicas/aliases，见 issue #29）。
	if err := s.writer.EnsureIndex(ctx); err != nil {
		return fmt.Errorf("consumer: ensure index: %w", err)
	}
	// 🔴 mapping-compat fail-closed 断言（§6.4）：方案 B 新增 payloadRaw / richText /
	// mergeForward.msgs.{from,timestamp}，dynamic:strict 下若漏迁这些字段会启动后每条 bulk 4xx
	// 静默全量塌。启动期校验 live mapping 含本期所有新字段路径，缺则**拒启动**（loud crash），
	// 不静默灌 4xx。与 EnsureIndex 的存在性校验独立（存在即放行，字段缺失才拒启动）。
	if err := s.writer.AssertLiveMappingCompatible(ctx); err != nil {
		return fmt.Errorf("consumer: mapping-compat assertion: %w", err)
	}
	return s.proc.Run(ctx)
}

// Close 释放底层资源（Kafka reader/writer + ES 客户端）。
func (s *Service) Close() error {
	var firstErr error
	if err := s.source.Close(); err != nil {
		firstErr = err
	}
	if err := s.dlqSink.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := s.writer.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}
