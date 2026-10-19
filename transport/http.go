// Package transport carries Raft RPCs over HTTP with JSON bodies.
package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/Alkarim786/raft-kv-store/raft"
)

// HTTP implements raft.Transport. Peers maps node ID to base URL.
type HTTP struct {
	Peers  map[string]string
	client *http.Client
}

// NewHTTP returns a transport with keep-alive connection pooling.
func NewHTTP(peers map[string]string) *HTTP {
	return &HTTP{
		Peers: peers,
		client: &http.Client{Transport: &http.Transport{
			MaxIdleConnsPerHost: 16,
			IdleConnTimeout:     90 * time.Second,
			DialContext:         (&net.Dialer{Timeout: time.Second}).DialContext,
		}},
	}
}

func post[Req, Resp any](ctx context.Context, t *HTTP, peer, path string, req *Req) (*Resp, error) {
	base, ok := t.Peers[peer]
	if !ok {
		return nil, fmt.Errorf("unknown peer %q", peer)
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	hr, err := http.NewRequestWithContext(ctx, http.MethodPost, base+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	hr.Header.Set("Content-Type", "application/json")
	resp, err := t.client.Do(hr)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s%s: HTTP %d", peer, path, resp.StatusCode)
	}
	var out Resp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (t *HTTP) RequestVote(ctx context.Context, p string, a *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	return post[raft.RequestVoteArgs, raft.RequestVoteReply](ctx, t, p, "/raft/vote", a)
}

func (t *HTTP) AppendEntries(ctx context.Context, p string, a *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	return post[raft.AppendEntriesArgs, raft.AppendEntriesReply](ctx, t, p, "/raft/append", a)
}

func (t *HTTP) InstallSnapshot(ctx context.Context, p string, a *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	return post[raft.InstallSnapshotArgs, raft.InstallSnapshotReply](ctx, t, p, "/raft/snapshot", a)
}

// Register mounts the receiving side of the RPCs on mux.
func Register(mux *http.ServeMux, h raft.Handler) {
	mux.HandleFunc("POST /raft/vote", serve(h.HandleRequestVote))
	mux.HandleFunc("POST /raft/append", serve(h.HandleAppendEntries))
	mux.HandleFunc("POST /raft/snapshot", serve(h.HandleInstallSnapshot))
}

func serve[Req, Resp any](fn func(*Req) (*Resp, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req Req
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<20)).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		resp, err := fn(&req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}
}
