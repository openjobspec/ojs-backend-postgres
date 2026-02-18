package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"nhooyr.io/websocket"

	"github.com/openjobspec/ojs-backend-postgres/internal/core"
)

func TestWSHandler_SubscribeAndReceive(t *testing.T) {
	backend := &mockBackend{}
	sub := newMockSubscriber()
	handler := NewWSHandler(backend, sub)

	srv := httptest.NewServer(http.HandlerFunc(handler.Handle))
	defer srv.Close()

	wsURL := "ws" + srv.URL[4:] // http -> ws
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	// Subscribe to a job channel
	subMsg := `{"action":"subscribe","channel":"job:job-123"}`
	err = conn.Write(ctx, websocket.MessageText, []byte(subMsg))
	if err != nil {
		t.Fatalf("write subscribe: %v", err)
	}

	// Read subscription confirmation
	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read confirmation: %v", err)
	}

	var resp wsResponse
	json.Unmarshal(data, &resp)
	if resp.Type != "subscribed" {
		t.Errorf("expected subscribed, got %s", resp.Type)
	}
	if resp.Channel != "job:job-123" {
		t.Errorf("expected channel job:job-123, got %s", resp.Channel)
	}

	// Send an event through the mock subscriber
	sub.jobCh <- core.NewStateChangedEvent("job-123", "default", "test", "available", "active")

	// Read the event
	_, data, err = conn.Read(ctx)
	if err != nil {
		t.Fatalf("read event: %v", err)
	}

	var eventResp wsResponse
	json.Unmarshal(data, &eventResp)
	if eventResp.Type != "event" {
		t.Errorf("expected event type, got %s", eventResp.Type)
	}
	if eventResp.Event != "job.state_changed" {
		t.Errorf("expected job.state_changed, got %s", eventResp.Event)
	}
}

func TestWSHandler_Unsubscribe(t *testing.T) {
	backend := &mockBackend{}
	sub := newMockSubscriber()
	handler := NewWSHandler(backend, sub)

	srv := httptest.NewServer(http.HandlerFunc(handler.Handle))
	defer srv.Close()

	wsURL := "ws" + srv.URL[4:]
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	// Subscribe
	err = conn.Write(ctx, websocket.MessageText, []byte(`{"action":"subscribe","channel":"all"}`))
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	_, _, _ = conn.Read(ctx) // read confirmation

	// Unsubscribe
	err = conn.Write(ctx, websocket.MessageText, []byte(`{"action":"unsubscribe","channel":"all"}`))
	if err != nil {
		t.Fatalf("write unsubscribe: %v", err)
	}

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read unsubscribe: %v", err)
	}

	var resp wsResponse
	json.Unmarshal(data, &resp)
	if resp.Type != "unsubscribed" {
		t.Errorf("expected unsubscribed, got %s", resp.Type)
	}
}

func TestWSHandler_InvalidAction(t *testing.T) {
	backend := &mockBackend{}
	sub := newMockSubscriber()
	handler := NewWSHandler(backend, sub)

	srv := httptest.NewServer(http.HandlerFunc(handler.Handle))
	defer srv.Close()

	wsURL := "ws" + srv.URL[4:]
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	err = conn.Write(ctx, websocket.MessageText, []byte(`{"action":"invalid"}`))
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	var resp wsResponse
	json.Unmarshal(data, &resp)
	if resp.Type != "error" {
		t.Errorf("expected error, got %s", resp.Type)
	}
}

func TestWSHandler_InvalidChannel(t *testing.T) {
	backend := &mockBackend{}
	sub := newMockSubscriber()
	handler := NewWSHandler(backend, sub)

	srv := httptest.NewServer(http.HandlerFunc(handler.Handle))
	defer srv.Close()

	wsURL := "ws" + srv.URL[4:]
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	err = conn.Write(ctx, websocket.MessageText, []byte(`{"action":"subscribe","channel":"invalid:something"}`))
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	var resp wsResponse
	json.Unmarshal(data, &resp)
	if resp.Type != "error" {
		t.Errorf("expected error, got %s", resp.Type)
	}
	if resp.Code != "invalid_request" {
		t.Errorf("expected invalid_request code, got %s", resp.Code)
	}
}

func TestWSHandler_JobNotFound(t *testing.T) {
	backend := &mockBackend{
		infoFunc: func(ctx context.Context, jobID string) (*core.Job, error) {
			return nil, core.NewNotFoundError("Job", jobID)
		},
	}
	sub := newMockSubscriber()
	handler := NewWSHandler(backend, sub)

	srv := httptest.NewServer(http.HandlerFunc(handler.Handle))
	defer srv.Close()

	wsURL := "ws" + srv.URL[4:]
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	err = conn.Write(ctx, websocket.MessageText, []byte(`{"action":"subscribe","channel":"job:nonexistent"}`))
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	var resp wsResponse
	json.Unmarshal(data, &resp)
	if resp.Type != "error" {
		t.Errorf("expected error, got %s", resp.Type)
	}
	if resp.Code != "not_found" {
		t.Errorf("expected not_found code, got %s", resp.Code)
	}
}

func TestWSHandler_InvalidJSON(t *testing.T) {
	backend := &mockBackend{}
	sub := newMockSubscriber()
	handler := NewWSHandler(backend, sub)

	srv := httptest.NewServer(http.HandlerFunc(handler.Handle))
	defer srv.Close()

	wsURL := "ws" + srv.URL[4:]
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	err = conn.Write(ctx, websocket.MessageText, []byte(`{invalid json`))
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	var resp wsResponse
	json.Unmarshal(data, &resp)
	if resp.Type != "error" {
		t.Errorf("expected error, got %s", resp.Type)
	}
}
