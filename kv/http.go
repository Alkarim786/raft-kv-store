package kv

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"
)

// Server exposes a Store over HTTP:
//
//	GET    /kv/{key}         linearizable read
//	PUT    /kv/{key}         body = value
//	DELETE /kv/{key}
//	POST   /kv/{key}/cas     {"expect": "...", "value": "..."}
//	GET    /status
//
// Writes accept X-Client-ID and X-Seq headers for exactly-once retries.
// Non-leaders answer 421 with {"leader": "<url>"} so clients can redirect.
type Server struct {
	store   *Store
	leaders map[string]string // raft node ID -> client-facing URL
	timeout time.Duration
}

// NewServer builds the HTTP API. leaders maps node IDs to their API URLs.
func NewServer(store *Store, leaders map[string]string) *Server {
	return &Server{store: store, leaders: leaders, timeout: 3 * time.Second}
}

// Register mounts the API routes on mux.
func (s *Server) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /kv/{key}", s.get)
	mux.HandleFunc("PUT /kv/{key}", s.put)
	mux.HandleFunc("DELETE /kv/{key}", s.del)
	mux.HandleFunc("POST /kv/{key}/cas", s.cas)
	mux.HandleFunc("GET /status", s.status)
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
	defer cancel()
	res, err := s.store.Get(ctx, r.PathValue("key"))
	s.reply(w, res, err)
}

func (s *Server) put(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
		return
	}
	s.write(w, r, Op{Kind: OpPut, Key: r.PathValue("key"), Value: string(body)})
}

func (s *Server) del(w http.ResponseWriter, r *http.Request) {
	s.write(w, r, Op{Kind: OpDelete, Key: r.PathValue("key")})
}

func (s *Server) cas(w http.ResponseWriter, r *http.Request) {
	var req struct{ Expect, Value string }
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.write(w, r, Op{Kind: OpCAS, Key: r.PathValue("key"), Expect: req.Expect, Value: req.Value})
}

func (s *Server) write(w http.ResponseWriter, r *http.Request, op Op) {
	op.ClientID = r.Header.Get("X-Client-ID")
	if seq := r.Header.Get("X-Seq"); seq != "" {
		n, err := strconv.ParseUint(seq, 10, 64)
		if err != nil {
			http.Error(w, "bad X-Seq", http.StatusBadRequest)
			return
		}
		op.Seq = n
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
	defer cancel()
	res, err := s.store.Submit(ctx, op)
	s.reply(w, res, err)
}

func (s *Server) reply(w http.ResponseWriter, res Result, err error) {
	w.Header().Set("Content-Type", "application/json")
	switch {
	case errors.Is(err, ErrWrongLeader):
		w.WriteHeader(http.StatusMisdirectedRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "not leader", "leader": s.leaders[s.store.rf.Leader()]})
	case errors.Is(err, ErrTimeout):
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "timeout"})
	case err != nil:
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
	default:
		_ = json.NewEncoder(w).Encode(res)
	}
}

func (s *Server) status(w http.ResponseWriter, _ *http.Request) {
	st := s.store.rf.Status()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"raft": st, "keys": s.store.Len(), "leaderURL": s.leaders[st.Leader]})
}
