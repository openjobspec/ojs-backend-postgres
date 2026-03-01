package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/openjobspec/ojs-backend-postgres/internal/core"
	"github.com/openjobspec/ojs-go-backend-common/httputil"
)

// HistoryHandler handles execution history API endpoints.
type HistoryHandler struct {
	backend interface {
		GetJobHistory(ctx context.Context, jobID string, limit int, cursor string) (*core.HistoryPage, error)
		GetJobLineage(ctx context.Context, jobID string) (*core.JobLineage, error)
		PurgeJobHistory(ctx context.Context, jobID string) error
	}
}

// NewHistoryHandler creates a new HistoryHandler.
func NewHistoryHandler(backend any) *HistoryHandler {
	type historyBackend interface {
		GetJobHistory(ctx context.Context, jobID string, limit int, cursor string) (*core.HistoryPage, error)
		GetJobLineage(ctx context.Context, jobID string) (*core.JobLineage, error)
		PurgeJobHistory(ctx context.Context, jobID string) error
	}
	if hb, ok := backend.(historyBackend); ok {
		return &HistoryHandler{backend: hb}
	}
	return &HistoryHandler{}
}

// GetJobHistory returns execution history for a specific job.
func (h *HistoryHandler) GetJobHistory(w http.ResponseWriter, r *http.Request) {
	if h.backend == nil {
		httputil.WriteError(w, http.StatusNotFound, core.NewNotFoundError("Feature", "execution-history"))
		return
	}
	jobID := chi.URLParam(r, "id")
	limit := 50
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil {
			limit = parsed
		}
	}
	cursor := r.URL.Query().Get("cursor")

	page, err := h.backend.GetJobHistory(r.Context(), jobID, limit, cursor)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, core.NewInternalError(err.Error()))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(page)
}

// GetJobLineage returns the parent-child relationship tree for a job.
func (h *HistoryHandler) GetJobLineage(w http.ResponseWriter, r *http.Request) {
	if h.backend == nil {
		httputil.WriteError(w, http.StatusNotFound, core.NewNotFoundError("Feature", "execution-history"))
		return
	}
	jobID := chi.URLParam(r, "id")
	lineage, err := h.backend.GetJobLineage(r.Context(), jobID)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, core.NewInternalError(err.Error()))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(lineage)
}

// DeleteJobHistory purges all history for a specific job (GDPR).
func (h *HistoryHandler) DeleteJobHistory(w http.ResponseWriter, r *http.Request) {
	if h.backend == nil {
		httputil.WriteError(w, http.StatusNotFound, core.NewNotFoundError("Feature", "execution-history"))
		return
	}
	jobID := chi.URLParam(r, "id")
	if err := h.backend.PurgeJobHistory(r.Context(), jobID); err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, core.NewInternalError(err.Error()))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"deleted": true, "job_id": jobID})
}
