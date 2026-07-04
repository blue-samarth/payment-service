package handlers

import (
	"context"
	"net/http"
)

type Pinger interface {
	Ping(ctx context.Context) error
}

type HealthHandler struct {
	db Pinger
}

func NewHealthHandler(db Pinger) *HealthHandler {
	return &HealthHandler{db: db}
}

type healthResponse struct {
	Status     string            `json:"status"`
	Components map[string]string `json:"components"`
}

func (h *HealthHandler) Health(w http.ResponseWriter, r *http.Request) {
	components := map[string]string{}
	healthy := true

	if err := h.db.Ping(r.Context()); err != nil {
		components["database"] = "unhealthy"
		healthy = false
	} else {
		components["database"] = "healthy"
	}

	status := http.StatusOK
	overall := "healthy"
	if !healthy {
		status = http.StatusServiceUnavailable
		overall = "unhealthy"
	}

	writeJSON(w, status, healthResponse{Status: overall, Components: components})
}
