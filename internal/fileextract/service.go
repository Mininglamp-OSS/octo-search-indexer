package fileextract

import (
	"context"
	"fmt"
	"log"
)

// Service 把 Kafka 消费侧 + DLQ + Extractor 组装成可运行单元。
// cmd/file-extractor/main.go 用。
type Service struct {
	proc      *Processor
	source    *kafkaSource
	dlqSink   *kafkaDLQSink
	extractor *Extractor
}

// NewService 装配真实组件（连 Kafka + OS，起 Tika HTTP client + download client）。
func NewService(cfg ServiceConfig) (*Service, error) {
	source, err := newKafkaSource(KafkaSourceConfig{
		Brokers: cfg.Brokers,
		Topic:   cfg.Topic,
		GroupID: cfg.GroupID,
	})
	if err != nil {
		return nil, fmt.Errorf("fileextract: build kafka source: %w", err)
	}
	dlqSink, err := newKafkaDLQSink(cfg.Brokers, cfg.DLQTopic)
	if err != nil {
		if cerr := source.Close(); cerr != nil {
			log.Printf("fileextract: close source after DLQ init failure: %v", cerr)
		}
		return nil, err
	}
	extractor, err := NewExtractor(cfg)
	if err != nil {
		if cerr := source.Close(); cerr != nil {
			log.Printf("fileextract: close source after extractor init failure: %v", cerr)
		}
		if cerr := dlqSink.Close(); cerr != nil {
			log.Printf("fileextract: close dlq after extractor init failure: %v", cerr)
		}
		return nil, fmt.Errorf("fileextract: build extractor: %w", err)
	}
	proc := NewProcessor(source, dlqSink, extractor, cfg)
	return &Service{
		proc:      proc,
		source:    source,
		dlqSink:   dlqSink,
		extractor: extractor,
	}, nil
}

// Run 运行到 ctx 取消。
func (s *Service) Run(ctx context.Context) error { return s.proc.Run(ctx) }

// Close 关闭底层 Kafka client（Reader + DLQ Writer）。
func (s *Service) Close() error {
	var firstErr error
	if err := s.source.Close(); err != nil {
		firstErr = err
	}
	if err := s.dlqSink.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}
