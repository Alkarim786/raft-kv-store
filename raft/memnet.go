package raft

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// ErrUnreachable is returned by MemNetwork when a message cannot be delivered.
var ErrUnreachable = errors.New("memnet: peer unreachable")

// MemNetwork is an in-process network for tests with fault injection:
// disconnecting nodes, partitions, message loss, delays and reordering.
// Messages are JSON round-tripped so nodes never share memory, exactly as
// over a real wire.
type MemNetwork struct {
	mu        sync.Mutex
	handlers  map[string]Handler
	up        map[string]bool
	cut       map[[2]string]bool
	reliable  bool
	longDelay bool
	rng       *rand.Rand
	rpcs      atomic.Int64
	bytes     atomic.Int64
}

// NewMemNetwork returns a reliable network with no nodes.
func NewMemNetwork() *MemNetwork {
	return &MemNetwork{
		handlers: map[string]Handler{},
		up:       map[string]bool{},
		cut:      map[[2]string]bool{},
		reliable: true,
		rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Register attaches (or replaces, on restart) the handler for id and connects it.
func (m *MemNetwork) Register(id string, h Handler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handlers[id] = h
	m.up[id] = true
}

// Connect / Disconnect toggle all traffic to and from id.
func (m *MemNetwork) Connect(id string)    { m.setUp(id, true) }
func (m *MemNetwork) Disconnect(id string) { m.setUp(id, false) }

func (m *MemNetwork) setUp(id string, v bool) {
	m.mu.Lock()
	m.up[id] = v
	m.mu.Unlock()
}

// Partition splits the cluster into groups; links between groups are cut.
// Partition() with no arguments heals everything.
func (m *MemNetwork) Partition(groups ...[]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cut = map[[2]string]bool{}
	for i, g := range groups {
		for j, h := range groups {
			if i == j {
				continue
			}
			for _, a := range g {
				for _, b := range h {
					m.cut[[2]string{a, b}] = true
				}
			}
		}
	}
}

// SetReliable toggles random loss (10% of requests and of replies) and
// delays of up to 27ms.
func (m *MemNetwork) SetReliable(v bool) {
	m.mu.Lock()
	m.reliable = v
	m.mu.Unlock()
}

// SetLongDelays makes undeliverable messages wait a long time before failing,
// which reorders traffic heavily.
func (m *MemNetwork) SetLongDelays(v bool) {
	m.mu.Lock()
	m.longDelay = v
	m.mu.Unlock()
}

// RPCCount returns the number of RPCs attempted so far.
func (m *MemNetwork) RPCCount() int64 { return m.rpcs.Load() }

// Bytes returns the number of request bytes sent so far.
func (m *MemNetwork) Bytes() int64 { return m.bytes.Load() }

func (m *MemNetwork) connected(from, to string) bool {
	return m.up[from] && m.up[to] && !m.cut[[2]string{from, to}]
}

func (m *MemNetwork) rand(n int) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.rng.Intn(n)
}

// Transport returns the Transport a node with the given id uses to send.
func (m *MemNetwork) Transport(from string) Transport { return &memTransport{net: m, from: from} }

func call[Req, Resp any](ctx context.Context, m *MemNetwork, from, to string, req *Req,
	handle func(Handler, *Req) (*Resp, error)) (*Resp, error) {
	m.rpcs.Add(1)
	wire, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	m.bytes.Add(int64(len(wire)))

	m.mu.Lock()
	ok := m.connected(from, to)
	reliable, long := m.reliable, m.longDelay
	h := m.handlers[to]
	m.mu.Unlock()

	fail := func() (*Resp, error) {
		ms := 0
		if long {
			ms = m.rand(700)
		} else {
			ms = m.rand(100)
		}
		select {
		case <-time.After(time.Duration(ms) * time.Millisecond):
		case <-ctx.Done():
		}
		return nil, ErrUnreachable
	}
	if !ok || h == nil {
		return fail()
	}
	if !reliable {
		time.Sleep(time.Duration(m.rand(27)) * time.Millisecond)
		if m.rand(1000) < 100 {
			return fail()
		}
	}
	var in Req
	if err := json.Unmarshal(wire, &in); err != nil {
		return nil, err
	}
	type result struct {
		r   *Resp
		err error
	}
	done := make(chan result, 1)
	go func() {
		r, err := handle(h, &in)
		done <- result{r, err}
	}()
	var res result
	select {
	case res = <-done:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if res.err != nil {
		return nil, res.err
	}
	m.mu.Lock()
	ok = m.connected(from, to) && m.handlers[to] == h
	m.mu.Unlock()
	if !ok || (!reliable && m.rand(1000) < 100) {
		return nil, ErrUnreachable // reply lost
	}
	out, err := json.Marshal(res.r)
	if err != nil {
		return nil, err
	}
	var resp Resp
	if err := json.Unmarshal(out, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

type memTransport struct {
	net  *MemNetwork
	from string
}

func (t *memTransport) RequestVote(ctx context.Context, peer string, a *RequestVoteArgs) (*RequestVoteReply, error) {
	return call(ctx, t.net, t.from, peer, a, Handler.HandleRequestVote)
}

func (t *memTransport) AppendEntries(ctx context.Context, peer string, a *AppendEntriesArgs) (*AppendEntriesReply, error) {
	return call(ctx, t.net, t.from, peer, a, Handler.HandleAppendEntries)
}

func (t *memTransport) InstallSnapshot(ctx context.Context, peer string, a *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	return call(ctx, t.net, t.from, peer, a, Handler.HandleInstallSnapshot)
}
