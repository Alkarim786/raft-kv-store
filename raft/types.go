// Package raft implements the Raft consensus algorithm (Ongaro & Ousterhout,
// "In Search of an Understandable Consensus Algorithm", 2014): leader
// election, log replication, persistence, log compaction with snapshots, and
// ReadIndex linearizable reads.
package raft

import "errors"

// State is a node's role.
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	return [...]string{"follower", "candidate", "leader"}[s]
}

// Entry is one log entry. A nil Command is the no-op a new leader appends to
// commit entries from earlier terms (Raft §8).
type Entry struct {
	Index   uint64 `json:"i"`
	Term    uint64 `json:"t"`
	Command []byte `json:"c,omitempty"`
}

// ApplyMsg is delivered on the apply channel, in log order, once an entry is
// committed, or when a snapshot from the leader replaces the state machine.
type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	Index        uint64
	Term         uint64

	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex uint64
	SnapshotTerm  uint64
}

// RequestVoteArgs is the RequestVote RPC request (§5.2).
type RequestVoteArgs struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidate"`
	LastLogIndex uint64 `json:"lastIndex"`
	LastLogTerm  uint64 `json:"lastTerm"`
}

// RequestVoteReply is the RequestVote RPC response.
type RequestVoteReply struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"granted"`
}

// AppendEntriesArgs is the AppendEntries RPC request (§5.3); with no entries
// it is a heartbeat.
type AppendEntriesArgs struct {
	Term         uint64  `json:"term"`
	LeaderID     string  `json:"leader"`
	PrevLogIndex uint64  `json:"prevIndex"`
	PrevLogTerm  uint64  `json:"prevTerm"`
	Entries      []Entry `json:"entries,omitempty"`
	LeaderCommit uint64  `json:"commit"`
}

// AppendEntriesReply carries ConflictTerm/ConflictIndex so the leader can
// skip back a whole term per round trip instead of one entry.
type AppendEntriesReply struct {
	Term          uint64 `json:"term"`
	Success       bool   `json:"success"`
	ConflictTerm  uint64 `json:"conflictTerm,omitempty"`
	ConflictIndex uint64 `json:"conflictIndex,omitempty"`
}

// InstallSnapshotArgs is the InstallSnapshot RPC request (§7). Snapshots are
// sent in one piece; they are small for this KV store.
type InstallSnapshotArgs struct {
	Term              uint64 `json:"term"`
	LeaderID          string `json:"leader"`
	LastIncludedIndex uint64 `json:"lastIndex"`
	LastIncludedTerm  uint64 `json:"lastTerm"`
	Data              []byte `json:"data"`
}

// InstallSnapshotReply is the InstallSnapshot RPC response.
type InstallSnapshotReply struct {
	Term uint64 `json:"term"`
}

// Status is a point-in-time view of a node, for observability.
type Status struct {
	ID            string `json:"id"`
	State         string `json:"state"`
	Term          uint64 `json:"term"`
	Leader        string `json:"leader"`
	CommitIndex   uint64 `json:"commitIndex"`
	LastApplied   uint64 `json:"lastApplied"`
	LastLogIndex  uint64 `json:"lastLogIndex"`
	SnapshotIndex uint64 `json:"snapshotIndex"`
	LogEntries    int    `json:"logEntries"`
}

var (
	// ErrNotLeader is returned by leader-only operations on other nodes.
	ErrNotLeader = errors.New("raft: not the leader")
	// ErrNotReady means a new leader hasn't committed an entry in its term
	// yet, so it can't serve ReadIndex. Retry shortly.
	ErrNotReady = errors.New("raft: leader not ready")
	// ErrStopped is returned after Stop.
	ErrStopped = errors.New("raft: node stopped")
)
