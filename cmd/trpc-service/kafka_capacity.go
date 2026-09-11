package main

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/twmb/franz-go/pkg/kadm"
)

const maxKafkaTopicPartitions = 4096

type kafkaCapacityAdmin interface {
	ListTopics(context.Context, ...string) (kadm.TopicDetails, error)
	Lag(context.Context, ...string) (kadm.DescribedGroupLags, error)
}

type kafkaCapacityRecorder interface {
	RecordKafkaCapacity(context.Context, metrics.KafkaCapacityAttributes)
	RecordKafkaCapacityFailure(context.Context, string, string)
}

type kafkaCapacityMonitor struct {
	admin    kafkaCapacityAdmin
	recorder kafkaCapacityRecorder
	topic    string
	group    string
	interval time.Duration
}

func kafkaPartitionRequirement(getenv environment, workerSlots int) (int, bool, error) {
	if workerSlots <= 0 {
		return 0, false, fmt.Errorf("worker slots must be positive")
	}
	raw := strings.TrimSpace(getenv("KAFKA_TOPIC_PARTITIONS"))
	if raw == "" {
		return workerSlots, false, nil
	}
	partitions, err := strconv.Atoi(raw)
	if err != nil || partitions <= 0 || partitions > maxKafkaTopicPartitions {
		return 0, false, fmt.Errorf("KAFKA_TOPIC_PARTITIONS must be between 1 and %d", maxKafkaTopicPartitions)
	}
	if partitions < workerSlots {
		return 0, false, fmt.Errorf("KAFKA_TOPIC_PARTITIONS (%d) must not be lower than WORKER_CONCURRENCY (%d)", partitions, workerSlots)
	}
	return partitions, true, nil
}

func newKafkaCapacityMonitor(admin kafkaCapacityAdmin, recorder kafkaCapacityRecorder, topic, group string, interval time.Duration) (*kafkaCapacityMonitor, error) {
	if admin == nil || recorder == nil || strings.TrimSpace(topic) == "" || strings.TrimSpace(group) == "" || interval <= 0 {
		return nil, fmt.Errorf("Kafka capacity monitor configuration is incomplete")
	}
	return &kafkaCapacityMonitor{admin: admin, recorder: recorder, topic: strings.TrimSpace(topic), group: strings.TrimSpace(group), interval: interval}, nil
}

func validateKafkaTopicCapacity(ctx context.Context, admin kafkaCapacityAdmin, topic string, minimumPartitions int) error {
	if admin == nil || strings.TrimSpace(topic) == "" || minimumPartitions <= 0 {
		return fmt.Errorf("Kafka topic capacity validation is incomplete")
	}
	details, err := admin.ListTopics(ctx, topic)
	if err != nil {
		return fmt.Errorf("list Kafka topic %q: %w", topic, err)
	}
	detail, ok := details[topic]
	if !ok {
		return fmt.Errorf("Kafka topic %q does not exist; provision it explicitly with at least %d partitions", topic, minimumPartitions)
	}
	if detail.Err != nil {
		return fmt.Errorf("inspect Kafka topic %q: %w", topic, detail.Err)
	}
	if len(detail.Partitions) < minimumPartitions {
		return fmt.Errorf("Kafka topic %q has %d partitions, below required minimum %d", topic, len(detail.Partitions), minimumPartitions)
	}
	return nil
}

func (m *kafkaCapacityMonitor) sample(ctx context.Context) (metrics.KafkaCapacityAttributes, error) {
	details, err := m.admin.ListTopics(ctx, m.topic)
	if err != nil {
		return metrics.KafkaCapacityAttributes{}, fmt.Errorf("list Kafka topic: %w", err)
	}
	detail, ok := details[m.topic]
	if !ok || detail.Err != nil {
		if ok && detail.Err != nil {
			return metrics.KafkaCapacityAttributes{}, fmt.Errorf("inspect Kafka topic: %w", detail.Err)
		}
		return metrics.KafkaCapacityAttributes{}, fmt.Errorf("Kafka topic %q not found", m.topic)
	}
	lags, err := m.admin.Lag(ctx, m.group)
	if err != nil {
		return metrics.KafkaCapacityAttributes{}, fmt.Errorf("read Kafka consumer lag: %w", err)
	}
	groupLag, ok := lags[m.group]
	if !ok {
		return metrics.KafkaCapacityAttributes{}, fmt.Errorf("Kafka consumer group %q not found", m.group)
	}
	if err := groupLag.Error(); err != nil {
		return metrics.KafkaCapacityAttributes{}, fmt.Errorf("read Kafka consumer group %q: %w", m.group, err)
	}
	var lag int64
	for _, partition := range groupLag.Lag[m.topic] {
		if partition.Err != nil {
			return metrics.KafkaCapacityAttributes{}, fmt.Errorf("read Kafka lag for %s/%d: %w", m.topic, partition.Partition, partition.Err)
		}
		if partition.Lag > 0 {
			lag += partition.Lag
		}
	}
	return metrics.KafkaCapacityAttributes{Topic: m.topic, ConsumerGroup: m.group, Lag: lag, Partitions: len(detail.Partitions)}, nil
}

func (m *kafkaCapacityMonitor) Run(ctx context.Context) {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		capacity, err := m.sample(ctx)
		if err != nil {
			if ctx.Err() == nil {
				m.recorder.RecordKafkaCapacityFailure(ctx, m.topic, m.group)
				slog.Warn("sample Kafka capacity", "error", err)
			}
		} else {
			m.recorder.RecordKafkaCapacity(ctx, capacity)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
