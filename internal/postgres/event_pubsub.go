package postgres

import (
	"encoding/json"
	"log/slog"
	"sync"

	"github.com/openjobspec/ojs-backend-postgres/internal/core"
)

// EventPubSubBroker implements core.EventPublisher and core.EventSubscriber
// using in-memory fan-out. Events published within this process are delivered
// to all matching subscribers.
type EventPubSubBroker struct {
	mu   sync.RWMutex
	subs map[*eventSubscription]struct{}
}

type eventSubscription struct {
	ch      chan *core.JobEvent
	filter  eventFilter
	closeFn func()
}

type eventFilter struct {
	jobID string // if set, only events for this job
	queue string // if set, only events for this queue
	all   bool   // if true, receive all events
}

// NewEventPubSubBroker creates a new EventPubSubBroker.
func NewEventPubSubBroker() *EventPubSubBroker {
	return &EventPubSubBroker{
		subs: make(map[*eventSubscription]struct{}),
	}
}

// PublishJobEvent publishes a job event to all matching in-memory subscribers.
func (b *EventPubSubBroker) PublishJobEvent(event *core.JobEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_ = data // validated marshalability

	b.mu.RLock()
	defer b.mu.RUnlock()

	for sub := range b.subs {
		if sub.filter.all ||
			(sub.filter.jobID != "" && sub.filter.jobID == event.JobID) ||
			(sub.filter.queue != "" && sub.filter.queue == event.Queue) {
			select {
			case sub.ch <- event:
			default:
				slog.Warn("dropping event, subscriber channel full",
					"job_id", event.JobID, "filter_job", sub.filter.jobID, "filter_queue", sub.filter.queue)
			}
		}
	}
	return nil
}

// SubscribeJob subscribes to events for a specific job.
func (b *EventPubSubBroker) SubscribeJob(jobID string) (<-chan *core.JobEvent, func(), error) {
	return b.subscribe(eventFilter{jobID: jobID})
}

// SubscribeQueue subscribes to events for all jobs in a queue.
func (b *EventPubSubBroker) SubscribeQueue(queue string) (<-chan *core.JobEvent, func(), error) {
	return b.subscribe(eventFilter{queue: queue})
}

// SubscribeAll subscribes to all events.
func (b *EventPubSubBroker) SubscribeAll() (<-chan *core.JobEvent, func(), error) {
	return b.subscribe(eventFilter{all: true})
}

func (b *EventPubSubBroker) subscribe(f eventFilter) (<-chan *core.JobEvent, func(), error) {
	ch := make(chan *core.JobEvent, 64)
	sub := &eventSubscription{
		ch:     ch,
		filter: f,
	}

	b.mu.Lock()
	b.subs[sub] = struct{}{}
	b.mu.Unlock()

	unsub := func() {
		b.mu.Lock()
		delete(b.subs, sub)
		b.mu.Unlock()
		close(ch)
	}
	sub.closeFn = unsub

	return ch, unsub, nil
}

// Close shuts down the broker and closes all subscriber channels.
func (b *EventPubSubBroker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for sub := range b.subs {
		close(sub.ch)
		delete(b.subs, sub)
	}
	return nil
}
