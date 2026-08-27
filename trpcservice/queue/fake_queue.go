package queue

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type FakeQueueConfig struct {
	Now       func() time.Time
	MaxJobAge time.Duration
}

type fakeEntryState uint8

const (
	fakeQueued fakeEntryState = iota + 1
	fakeInflight
	fakeAcked
	fakeDiscarded
)

type fakeEntry struct {
	envelope       JobEnvelope
	job            AgentJob
	attempt        int
	state          fakeEntryState
	activeToken    string
	visibleUntil   time.Time
	availableAt    time.Time
	lastAckedToken string
}

// FakeQueue is an in-memory Queue contract test double. It deliberately uses
// operation-time clock checks instead of timer goroutines so Close has no
// hidden workers to drain.
type FakeQueue struct {
	mu        sync.Mutex
	entries   []*fakeEntry
	closed    bool
	nextID    uint64
	notify    chan struct{}
	now       func() time.Time
	maxJobAge time.Duration
}

func NewFakeQueue(config FakeQueueConfig) *FakeQueue {
	now := config.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	maxAge := config.MaxJobAge
	if maxAge <= 0 {
		maxAge = DefaultJobMaxAge
	}
	return &FakeQueue{notify: make(chan struct{}, 1), now: now, maxJobAge: maxAge}
}

var _ JobQueue = (*FakeQueue)(nil)

func (q *FakeQueue) Enqueue(ctx context.Context, job AgentJob) (QueueReceipt, error) {
	if err := contextError(ctx); err != nil {
		return QueueReceipt{}, err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return QueueReceipt{}, ErrQueueClosed
	}
	if err := job.ValidateAt(q.now().UTC(), q.maxJobAge); err != nil {
		return QueueReceipt{}, err
	}
	envelope, err := EncodeJobAt(job, q.now().UTC(), q.maxJobAge)
	if err != nil {
		return QueueReceipt{}, err
	}
	q.nextID++
	entry := &fakeEntry{envelope: envelope, job: cloneJob(job), attempt: job.Attempt, state: fakeQueued, availableAt: q.now().UTC()}
	q.entries = append(q.entries, entry)
	q.signalLocked()
	return QueueReceipt{Accepted: true, ReceiptID: fmt.Sprintf("receipt-%d", q.nextID), JobID: job.JobID, Attempt: job.Attempt}, nil
}

// EncodeJobAt is the clock-aware form used by FakeQueue validation.
func EncodeJobAt(job AgentJob, now time.Time, maxAge time.Duration) (JobEnvelope, error) {
	if err := job.ValidateAt(now, maxAge); err != nil {
		return JobEnvelope{}, err
	}
	payload, err := marshalJob(job)
	if err != nil {
		return JobEnvelope{}, err
	}
	return JobEnvelope{SchemaVersion: SchemaVersion, JobID: job.JobID, Payload: payload}, nil
}

func (q *FakeQueue) Receive(ctx context.Context, visibilityTimeout time.Duration) (Delivery, error) {
	if visibilityTimeout <= 0 {
		return Delivery{}, fmt.Errorf("%w: visibility timeout must be positive", ErrInvalidDelivery)
	}
	for {
		if err := contextError(ctx); err != nil {
			return Delivery{}, err
		}
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return Delivery{}, ErrQueueClosed
		}
		now := q.now().UTC()
		q.requeueExpiredLocked(now)
		for _, entry := range q.entries {
			if entry.state != fakeQueued || entry.availableAt.After(now) {
				continue
			}
			q.nextID++
			token := fmt.Sprintf("delivery-%d", q.nextID)
			entry.state = fakeInflight
			entry.activeToken = token
			entry.visibleUntil = now.Add(visibilityTimeout)
			delivery := Delivery{
				DeliveryID: token, Envelope: cloneEnvelope(entry.envelope), Job: cloneJob(entry.job),
				Attempt: entry.attempt, ReceivedAt: now, VisibleUntil: entry.visibleUntil,
			}
			q.mu.Unlock()
			return delivery, nil
		}
		notify := q.notify
		q.mu.Unlock()
		select {
		case <-ctx.Done():
			return Delivery{}, ctx.Err()
		case <-notify:
		}
	}
}

func (q *FakeQueue) Ack(ctx context.Context, delivery Delivery) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	entry := q.entryForDeliveryLocked(delivery)
	if entry == nil {
		return ErrDeliveryExpired
	}
	if entry.state == fakeAcked {
		if entry.lastAckedToken == delivery.DeliveryID {
			return nil
		}
		return ErrDeliveryExpired
	}
	if entry.state != fakeInflight || entry.activeToken != delivery.DeliveryID {
		return ErrDeliveryFinished
	}
	entry.state = fakeAcked
	entry.lastAckedToken = delivery.DeliveryID
	entry.activeToken = ""
	q.signalLocked()
	return nil
}

func (q *FakeQueue) Nack(ctx context.Context, delivery Delivery, options NackOptions) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := options.Validate(); err != nil {
		return err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	entry := q.entryForDeliveryLocked(delivery)
	if entry == nil {
		return ErrDeliveryExpired
	}
	if entry.state != fakeInflight || entry.activeToken != delivery.DeliveryID {
		return ErrDeliveryFinished
	}
	entry.activeToken = ""
	if options.Requeue {
		entry.state = fakeQueued
		entry.attempt++
		entry.availableAt = q.now().UTC().Add(options.RetryAfter)
	} else {
		entry.state = fakeDiscarded
	}
	q.signalLocked()
	return nil
}

func (q *FakeQueue) ExtendVisibility(ctx context.Context, delivery Delivery, extension time.Duration) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if extension <= 0 || extension > q.maxJobAge {
		return fmt.Errorf("%w: invalid visibility extension", ErrInvalidDelivery)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	entry := q.entryForDeliveryLocked(delivery)
	if entry == nil {
		return ErrDeliveryExpired
	}
	now := q.now().UTC()
	if entry.state != fakeInflight || entry.activeToken != delivery.DeliveryID || !entry.visibleUntil.After(now) {
		return ErrDeliveryExpired
	}
	entry.visibleUntil = now.Add(extension)
	return nil
}

func (q *FakeQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil
	}
	q.closed = true
	q.signalLocked()
	return nil
}

func (q *FakeQueue) entryForDeliveryLocked(delivery Delivery) *fakeEntry {
	for _, entry := range q.entries {
		if entry.envelope.JobID == delivery.Job.JobID || entry.envelope.JobID == delivery.Envelope.JobID {
			return entry
		}
	}
	return nil
}

func (q *FakeQueue) requeueExpiredLocked(now time.Time) {
	for _, entry := range q.entries {
		if entry.state == fakeInflight && !entry.visibleUntil.After(now) {
			entry.state = fakeQueued
			entry.activeToken = ""
			entry.attempt++
			entry.availableAt = now
		}
	}
}

func (q *FakeQueue) signalLocked() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return context.Canceled
	}
	return ctx.Err()
}

func cloneEnvelope(envelope JobEnvelope) JobEnvelope {
	envelope.Payload = append([]byte(nil), envelope.Payload...)
	return envelope
}

func cloneJob(job AgentJob) AgentJob {
	job.Tenant.Permissions = append([]string(nil), job.Tenant.Permissions...)
	job.Message.ToolCalls = append([]ToolCallDTO(nil), job.Message.ToolCalls...)
	return job
}
