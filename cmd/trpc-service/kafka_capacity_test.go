package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/twmb/franz-go/pkg/kadm"
)

type fakeKafkaCapacityAdmin struct {
	topics kadm.TopicDetails
	lags   kadm.DescribedGroupLags
	err    error
}

func (a fakeKafkaCapacityAdmin) ListTopics(context.Context, ...string) (kadm.TopicDetails, error) {
	if a.err != nil {
		return nil, a.err
	}
	return a.topics, nil
}

func (a fakeKafkaCapacityAdmin) Lag(context.Context, ...string) (kadm.DescribedGroupLags, error) {
	if a.err != nil {
		return nil, a.err
	}
	return a.lags, nil
}

type recordingKafkaCapacity struct {
	mu       sync.Mutex
	samples  []metrics.KafkaCapacityAttributes
	failures int
}

func (r *recordingKafkaCapacity) RecordKafkaCapacity(_ context.Context, sample metrics.KafkaCapacityAttributes) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.samples = append(r.samples, sample)
}

func (r *recordingKafkaCapacity) RecordKafkaCapacityFailure(context.Context, string, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failures++
}

func (r *recordingKafkaCapacity) sampleCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.samples)
}

func (r *recordingKafkaCapacity) failureCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.failures
}

func TestKafkaCapacityMonitorSamplesTopicPartitionsAndConsumerLag(t *testing.T) {
	topic := "agent.inbound.v1"
	group := "trpc-agent-worker"
	admin := fakeKafkaCapacityAdmin{
		topics: kadm.TopicDetails{topic: {Topic: topic, Partitions: kadm.PartitionDetails{
			0: {Topic: topic, Partition: 0}, 1: {Topic: topic, Partition: 1}, 2: {Topic: topic, Partition: 2}, 3: {Topic: topic, Partition: 3},
		}}},
		lags: kadm.DescribedGroupLags{group: {Group: group, Lag: kadm.GroupLag{topic: {
			0: {Topic: topic, Partition: 0, Lag: 2}, 1: {Topic: topic, Partition: 1, Lag: 3},
			2: {Topic: topic, Partition: 2, Lag: 0}, 3: {Topic: topic, Partition: 3, Lag: 5},
		}}}},
	}
	recorder := &recordingKafkaCapacity{}
	monitor, err := newKafkaCapacityMonitor(admin, recorder, topic, group, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	sample, err := monitor.sample(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if sample.Partitions != 4 || sample.Lag != 10 || sample.Topic != topic || sample.ConsumerGroup != group {
		t.Fatalf("capacity sample = %#v", sample)
	}
}

func TestKafkaCapacityMonitorRejectsMissingOrUndersizedTopic(t *testing.T) {
	topic := "agent.inbound.v1"
	for name, admin := range map[string]fakeKafkaCapacityAdmin{
		"missing":     {topics: kadm.TopicDetails{}},
		"topic error": {topics: kadm.TopicDetails{topic: {Topic: topic, Err: errors.New("metadata failed")}}},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateKafkaTopicCapacity(context.Background(), admin, topic, 4); err == nil {
				t.Fatal("validation succeeded")
			}
		})
	}
	admin := fakeKafkaCapacityAdmin{topics: kadm.TopicDetails{topic: {Topic: topic, Partitions: kadm.PartitionDetails{0: {}, 1: {}}}}}
	if err := validateKafkaTopicCapacity(context.Background(), admin, topic, 4); err == nil {
		t.Fatal("undersized topic validation succeeded")
	}
	if err := validateKafkaTopicCapacity(context.Background(), admin, topic, 2); err != nil {
		t.Fatalf("valid topic rejected: %v", err)
	}
}

func TestKafkaPartitionRequirement(t *testing.T) {
	for _, test := range []struct {
		value    string
		want     int
		explicit bool
		wantErr  bool
	}{
		{"", 4, false, false},
		{"80", 80, true, false},
		{"2", 0, false, true},
		{"0", 0, false, true},
		{"invalid", 0, false, true},
	} {
		got, explicit, err := kafkaPartitionRequirement(func(name string) string {
			if name == "KAFKA_TOPIC_PARTITIONS" {
				return test.value
			}
			return ""
		}, 4)
		if got != test.want || explicit != test.explicit || (err != nil) != test.wantErr {
			t.Fatalf("requirement(%q) = %d,%v,%v", test.value, got, explicit, err)
		}
	}
}

func TestKafkaCapacityMonitorRunPublishesImmediatelyAndStops(t *testing.T) {
	topic := "agent.inbound.v1"
	group := "trpc-agent-worker"
	admin := fakeKafkaCapacityAdmin{
		topics: kadm.TopicDetails{topic: {Topic: topic, Partitions: kadm.PartitionDetails{0: {}}}},
		lags:   kadm.DescribedGroupLags{group: {Group: group, Lag: kadm.GroupLag{topic: {0: {Lag: 7}}}}},
	}
	recorder := &recordingKafkaCapacity{}
	monitor, err := newKafkaCapacityMonitor(admin, recorder, topic, group, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { monitor.Run(ctx); close(done) }()
	deadline := time.Now().Add(time.Second)
	for recorder.sampleCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if recorder.sampleCount() == 0 {
		t.Fatal("capacity monitor did not publish initial sample")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("capacity monitor did not stop")
	}
}

func TestKafkaCapacityMonitorRunRecordsSamplingFailures(t *testing.T) {
	recorder := &recordingKafkaCapacity{}
	monitor, err := newKafkaCapacityMonitor(
		fakeKafkaCapacityAdmin{err: errors.New("metadata unavailable")},
		recorder,
		"agent.inbound.v1",
		"trpc-agent-worker",
		time.Hour,
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { monitor.Run(ctx); close(done) }()
	deadline := time.Now().Add(time.Second)
	for recorder.failureCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := recorder.failureCount(); got != 1 {
		t.Fatalf("sampling failures = %d, want 1", got)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("capacity monitor did not stop after failed sample")
	}
}
