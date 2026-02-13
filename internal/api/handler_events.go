package api

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/openjobspec/ojs-backend-postgres/internal/core"
)

// EventHandler handles Server-Sent Events for real-time job notifications.
type EventHandler struct {
	backend core.Backend
}

// NewEventHandler creates a new EventHandler.
func NewEventHandler(backend core.Backend) *EventHandler {
	return &EventHandler{backend: backend}
}

// Stream handles GET /ojs/v1/events?queues=default,critical
// It opens an SSE connection and pushes job_available events in real time.
func (h *EventHandler) Stream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		WriteError(w, http.StatusInternalServerError, core.NewInternalError("Streaming not supported."))
		return
	}

	// Parse queue filter from query params
	var queues []string
	if q := r.URL.Query().Get("queues"); q != "" {
		for _, s := range strings.Split(q, ",") {
			s = strings.TrimSpace(s)
			if s != "" {
				queues = append(queues, s)
			}
		}
	}

	events, cancel := h.backend.Subscribe(queues)
	defer cancel()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case queue, ok := <-events:
			if !ok {
				return
			}
			_, _ = fmt.Fprintf(w, "event: job_available\ndata: {\"queue\":%q}\n\n", queue)
			flusher.Flush()
		}
	}
}
