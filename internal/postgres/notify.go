package postgres

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const notifyChannel = "ojs_jobs_available"

// notifyJobAvailable sends a NOTIFY on the jobs channel for the given queue.
func notifyJobAvailable(ctx context.Context, pool *pgxpool.Pool, queue string) {
	_, err := pool.Exec(ctx, "SELECT pg_notify($1, $2)", notifyChannel, queue)
	if err != nil {
		log.Printf("[notify] error notifying queue %s: %v", queue, err)
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
				log.Printf("[listen] error acquiring connection: %v", err)
				time.Sleep(time.Second)
				continue
			}

			_, err = conn.Exec(ctx, "LISTEN "+notifyChannel)
			if err != nil {
				log.Printf("[listen] error subscribing: %v", err)
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
					log.Printf("[listen] notification error: %v", err)
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
