// Package kv is a linearizable key-value state machine replicated with Raft.
package kv

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/Alkarim786/raft-kv-store/raft"
)

// Op kinds.
const (
	OpPut    = "put"
	OpDelete = "delete"
	OpCAS    = "cas"
)

// Op is a client command replicated through the Raft log. (ClientID, Seq)
// identifies it for exactly-once semantics under retries.
type Op struct {
	Kind     string `json:"k"`
	Key      string `json:"key"`
	Value    string `json:"v,omitempty"`
	Expect   string `json:"e,omitempty"`
	ClientID string `json:"cid"`
	Seq      uint64 `json:"seq"`
}

// Result is the outcome of an Op or a read.
type Result struct {
	Value   string `json:"value,omitempty"`
	Found   bool   `json:"found"`
	Applied bool   `json:"applied"` // false when a CAS's expectation didn't match
}

var (
	// ErrWrongLeader means the op was not (or might not have been) committed
	// by this node as leader; retry elsewhere. Retrying is safe thanks to the
	// (ClientID, Seq) dedup.
	ErrWrongLeader = errors.New("kv: not leader")
	ErrTimeout     = errors.New("kv: timed out")
)

type session struct {
	Seq    uint64
	Result Result
}

type applied struct {
	op  Op
	res Result
}

// Store wraps a Raft node with a map state machine.
type Store struct {
	rf          *raft.Node
	storage     raft.Storage
	maxRaftSize int // snapshot when persisted Raft state exceeds this (0 = never)

	mu          sync.Mutex
	cond        *sync.Cond
	data        map[string]string
	sessions    map[string]session
	lastApplied uint64
	waiters     map[uint64]chan applied
	stopped     bool
}

// NewStore starts the apply loop. applyCh must be the channel given to rf.
func NewStore(rf *raft.Node, storage raft.Storage, applyCh <-chan raft.ApplyMsg, maxRaftSize int) *Store {
	s := &Store{
		rf: rf, storage: storage, maxRaftSize: maxRaftSize,
		data: map[string]string{}, sessions: map[string]session{}, waiters: map[uint64]chan applied{},
	}
	s.cond = sync.NewCond(&s.mu)
	go s.applyLoop(applyCh)
	return s
}

// Raft exposes the underlying node (for status endpoints).
func (s *Store) Raft() *raft.Node { return s.rf }

// Stop wakes any blocked readers; call after stopping the Raft node.
func (s *Store) Stop() {
	s.mu.Lock()
	s.stopped = true
	s.cond.Broadcast()
	for idx, w := range s.waiters {
		close(w)
		delete(s.waiters, idx)
	}
	s.mu.Unlock()
}

// Submit replicates a write and waits for it to be applied.
func (s *Store) Submit(ctx context.Context, op Op) (Result, error) {
	b, err := json.Marshal(op)
	if err != nil {
		return Result{}, err
	}
	ch := make(chan applied, 1)
	s.mu.Lock()
	// Register under the lock so the apply loop can't miss the waiter.
	idx, term, ok := s.rf.Start(b)
	if !ok {
		s.mu.Unlock()
		return Result{}, ErrWrongLeader
	}
	s.waiters[idx] = ch
	s.mu.Unlock()

	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case a, open := <-ch:
			if !open {
				return Result{}, ErrWrongLeader
			}
			// A different op at our index means we lost leadership and the
			// entry was overwritten.
			if a.op.ClientID != op.ClientID || a.op.Seq != op.Seq {
				return Result{}, ErrWrongLeader
			}
			return a.res, nil
		case <-tick.C:
			if t, leader := s.rf.State(); !leader || t != term {
				s.dropWaiter(idx, ch)
				return Result{}, ErrWrongLeader
			}
		case <-ctx.Done():
			s.dropWaiter(idx, ch)
			return Result{}, ErrTimeout
		}
	}
}

func (s *Store) dropWaiter(idx uint64, ch chan applied) {
	s.mu.Lock()
	if s.waiters[idx] == ch {
		delete(s.waiters, idx)
	}
	s.mu.Unlock()
}

// Get performs a linearizable read using Raft's ReadIndex: no log entry is
// written, but the read reflects every write that completed before it began.
func (s *Store) Get(ctx context.Context, key string) (Result, error) {
	idx, err := s.rf.ReadIndex(ctx)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrNotReady) {
			return Result{}, ErrWrongLeader
		}
		return Result{}, ErrTimeout
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.lastApplied < idx && !s.stopped {
		if ctx.Err() != nil {
			return Result{}, ErrTimeout
		}
		waitWithContext(ctx, s.cond)
	}
	if s.stopped {
		return Result{}, ErrWrongLeader
	}
	v, ok := s.data[key]
	return Result{Value: v, Found: ok, Applied: true}, nil
}

// waitWithContext is cond.Wait that also wakes when ctx is done.
func waitWithContext(ctx context.Context, c *sync.Cond) {
	stop := context.AfterFunc(ctx, func() {
		c.L.Lock()
		c.Broadcast()
		c.L.Unlock()
	})
	c.Wait()
	stop()
}

func (s *Store) applyLoop(ch <-chan raft.ApplyMsg) {
	for m := range ch {
		s.mu.Lock()
		switch {
		case m.SnapshotValid:
			if m.SnapshotIndex > s.lastApplied {
				s.restore(m.Snapshot)
				s.lastApplied = m.SnapshotIndex
				// Ops covered by the snapshot finished, but we can't tell their
				// results; their callers retry and dedup answers them.
				for idx, w := range s.waiters {
					if idx <= m.SnapshotIndex {
						close(w)
						delete(s.waiters, idx)
					}
				}
			}
		case m.CommandValid && m.Index > s.lastApplied:
			s.lastApplied = m.Index
			if m.Command != nil { // nil = leader no-op
				var op Op
				if err := json.Unmarshal(m.Command, &op); err == nil {
					res := s.apply(op)
					if w, ok := s.waiters[m.Index]; ok {
						w <- applied{op, res}
						delete(s.waiters, m.Index)
					}
				}
			}
			if s.maxRaftSize > 0 && s.storage.StateSize() >= s.maxRaftSize {
				s.rf.Snapshot(m.Index, s.encode())
			}
		}
		s.cond.Broadcast()
		s.mu.Unlock()
	}
}

// apply executes op exactly once per (ClientID, Seq).
func (s *Store) apply(op Op) Result {
	if sess, ok := s.sessions[op.ClientID]; ok && op.Seq <= sess.Seq {
		return sess.Result // duplicate (client retry): return the original result
	}
	var res Result
	switch op.Kind {
	case OpPut:
		_, res.Found = s.data[op.Key]
		s.data[op.Key] = op.Value
		res.Value, res.Applied = op.Value, true
	case OpDelete:
		res.Value, res.Found = s.data[op.Key]
		delete(s.data, op.Key)
		res.Applied = true
	case OpCAS:
		cur, ok := s.data[op.Key]
		res.Value, res.Found = cur, ok
		if (op.Expect == "" && !ok) || (ok && cur == op.Expect) {
			s.data[op.Key] = op.Value
			res.Value, res.Applied = op.Value, true
		}
	}
	if op.ClientID != "" {
		s.sessions[op.ClientID] = session{Seq: op.Seq, Result: res}
	}
	return res
}

type snapshot struct {
	Data     map[string]string
	Sessions map[string]session
}

func (s *Store) encode() []byte {
	var buf bytes.Buffer
	_ = gob.NewEncoder(&buf).Encode(snapshot{s.data, s.sessions})
	return buf.Bytes()
}

func (s *Store) restore(b []byte) {
	if len(b) == 0 {
		s.data, s.sessions = map[string]string{}, map[string]session{}
		return
	}
	var snap snapshot
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&snap); err != nil {
		panic("kv: corrupt snapshot: " + err.Error())
	}
	if snap.Data == nil {
		snap.Data = map[string]string{}
	}
	if snap.Sessions == nil {
		snap.Sessions = map[string]session{}
	}
	s.data, s.sessions = snap.Data, snap.Sessions
}

// Len returns the number of keys (for status).
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.data)
}
