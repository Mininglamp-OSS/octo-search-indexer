package consumer

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Mininglamp-OSS/octo-search-indexer/internal/esindex"
)

// Service 把 Kafka 消费侧 + ES 写入器 + DLQ 组装成可运行单元（cmd/es-indexer 用）。
type Service struct {
	proc    *Processor
	source  *kafkaSource
	dlqSink *kafkaDLQSink
	writer  esindex.Writer
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

	TransientBackoff time.Duration
	DLQMaxRetries    int
	DLQRetryBackoff  time.Duration
	DLQSpillDir      string
}

// logAlerter 是占位 alerter：记日志（阶段 7 接 Prometheus 计数 + 告警规则）。
type logAlerter struct{}

func (logAlerter) Alert(event string, detail string) {
	log.Printf("[ALERT] %s: %s", event, detail)
}

// NewService 装配真实组件（连 Kafka + OpenSearch）。
func NewService(cfg ServiceConfig) (*Service, error) {
	writer, err := esindex.NewWriter(esindex.Config{
		Addresses: cfg.ESAddresses,
		Index:     cfg.ESIndex,
		Username:  cfg.ESUsername,
		Password:  cfg.ESPassword,
	})
	if err != nil {
		return nil, fmt.Errorf("consumer: build ES writer: %w", err)
	}

	source, err := newKafkaSource(KafkaSourceConfig{
		Brokers: cfg.Brokers,
		Topic:   cfg.Topic,
		GroupID: cfg.GroupID,
	})
	if err != nil {
		return nil, err
	}

	dlqSink, err := newKafkaDLQSink(cfg.Brokers, cfg.DLQTopic)
	if err != nil {
		// 避免已建好的 source reader 泄漏（P2-4：构造器后段出错须回收前段资源）。
		if cerr := source.Close(); cerr != nil {
			log.Printf("consumer: close source after DLQ-sink init failure: %v", cerr)
		}
		return nil, err
	}

	alert := logAlerter{}
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
	})

	return &Service{proc: proc, source: source, dlqSink: dlqSink, writer: writer}, nil
}

// Run 运行消费循环直到 ctx 取消。启动时先幂等确保目标索引存在（带 mapping/中文分词）。
func (s *Service) Run(ctx context.Context) error {
	if err := s.writer.EnsureIndex(ctx); err != nil {
		return fmt.Errorf("consumer: ensure index: %w", err)
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
