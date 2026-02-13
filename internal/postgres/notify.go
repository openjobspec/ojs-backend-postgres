package postgres

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const notifyChannel = "ojs_jobs_available"

// notifyJobAvailable sends a NOTIFY on the jobs channel for the given queue.
func notifyJobAvailable(ctx context.Context, pool *pgxpool.Pool, queue string) {
	_, err := pool.Exec(ctx, "SELECT pg_notify($1, $2)", notifyChannel, queue)
	if err != nil {
		slog.Error("notify failed", "queue", queue, "error", err)
	}
}

// listenForNotifications starts a background goroutine that LISTENs on the
// jobs channel. This keeps a dedicated connection for LISTEN/NOTIFY.
// The onNotify callback is called with the queue name when a notification arrives.
func listenForNotifications(ctx context.Context, pool *pgxpool.Pool, onNotify func(queue string)) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			conn, err := pool.Acquire(ctx)
			if err != nil {
				slog.Error("listen: error acquiring connection", "error", err)
				time.Sleep(time.Second)
				continue
			}

			_, err = conn.Exec(ctx, "LISTEN "+notifyChannel)
			if err != nil {
				slog.Error("listen: error subscribing", "error", err)
				conn.Release()
				time.Sleep(time.Second)
				continue
			}

			for {
				notification, err := conn.Conn().WaitForNotification(ctx)
				if err != nil {
					if ctx.Err() != nil {
						conn.Release()
						return
					}
					slog.Error("listen: notification error", "error", err)
					conn.Release()
					time.Sleep(time.Second)
					break
				}

				if onNotify != nil {
					onNotify(notification.Payload)
				}
			}
		}
	}()
}

// subscriber represents a single SSE subscriber interested in queue notifications.
type subscriber struct {
	ch     chan string
	queues map[string]bool // empty means all queues
}

// pubSub manages subscribers for job availability notifications.
type pubSub struct {
	mu          sync.RWMutex
	subscribers map[*subscriber]struct{}
}

func newPubSub() *pubSub {
	return &pubSub{
		subscribers: make(map[*subscriber]struct{}),
	}
}

// subscribe registers a new subscriber for the given queues. If queues is empty,
// the subscriber receives notifications for all queues.
func (ps *pubSub) subscribe(queues []string) *subscriber {
	qm := make(map[string]bool, len(queues))
	for _, q := range queues {
		qm[q] = true
	}
	sub := &subscriber{
		ch:     make(chan string, 16),
		queues: qm,
	}
	ps.mu.Lock()
	ps.subscribers[sub] = struct{}{}
	ps.mu.Unlock()
	return sub
}

// unsubscribe removes a subscriber and closes its channel.
func (ps *pubSub) unsubscribe(sub *subscriber) {
	ps.mu.Lock()
	delete(ps.subscribers, sub)
	ps.mu.Unlock()
	close(sub.ch)
}

// publish sends a queue notification to all matching subscribers.
func (ps *pubSub) publish(queue string) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	for sub := range ps.subscribers {
		if len(sub.queues) == 0 || sub.queues[queue] {
			select {
			case sub.ch <- queue:
			default:
				// subscriber is slow, drop notification
			}
		}
	}
}

// Subscribe registers for real-time job availability events on the given queues.
// Returns a channel that receives queue names when jobs become available.
// Call Unsubscribe with the returned channel when done.
func (b *Backend) Subscribe(queues []string) (<-chan string, func()) {
	sub := b.pubsub.subscribe(queues)
	return sub.ch, func() { b.pubsub.unsubscribe(sub) }
}
