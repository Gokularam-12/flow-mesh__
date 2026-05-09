package api

import (
	"encoding/json"
	"net/http"

	"github.com/flowmesh/flowmesh/internal/xds"
)

type Handler struct {
	xds *xds.Server
}

func NewHandler(x *xds.Server) *Handler {
	return &Handler{xds: x}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
	switch r.URL.Path {
	case "/api/routes":
		json.NewEncoder(w).Encode(h.xds.GetRoutes())
	case "/api/events":
		json.NewEncoder(w).Encode(h.xds.GetEvents())
	default:
		http.NotFound(w, r)
	}
}
