package raft

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"log/slog"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Config configures a Node.
type Config struct {
	ID    string
	Peers []string // IDs of the other cluster members

	Storage   Storage
	Transport Transport
	ApplyCh   chan<- ApplyMsg

	ElectionTimeoutMin time.Duration // default 300ms
	ElectionTimeoutMax time.Duration // default 600ms
	HeartbeatInterval  time.Duration // default 50ms
	RPCTimeout         time.Duration // default 200ms
	MaxEntriesPerRPC   int           // default 256

	Logger *slog.Logger
}

func (c *Config) defaults() {
	if c.ElectionTimeoutMin == 0 {
		c.ElectionTimeoutMin = 300 * time.Millisecond
	}
	if c.ElectionTimeoutMax <= c.ElectionTimeoutMin {
		c.ElectionTimeoutMax = 2 * c.ElectionTimeoutMin
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 50 * time.Millisecond
	}
	if c.RPCTimeout == 0 {
		c.RPCTimeout = 200 * time.Millisecond
	}
	if c.MaxEntriesPerRPC == 0 {
		c.MaxEntriesPerRPC = 256
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.DiscardHandler)
	}
}

// Node is one Raft peer. All fields below mu are guarded by it.
type Node struct {
	cfg     Config
	id      string
	peers   []string
	log     *slog.Logger
	applyCh chan<- ApplyMsg
	stopped atomic.Bool
	done    chan struct{}
	wg      sync.WaitGroup

	mu        sync.Mutex
	applyCond *sync.Cond
	rng       *rand.Rand

	// Persistent state (Figure 2).
	term     uint64
	votedFor string
	entries  []Entry // entries[0] is the snapshot sentinel {Index: snapIndex, Term: snapTerm}
	snapshot []byte

	// Volatile state.
	state       State
	leader      string
	commitIndex uint64
	lastApplied uint64
	deadline    time.Time // election deadline

	// Leader state.
	nextIndex  map[string]uint64
	matchIndex map[string]uint64
	persisted  uint64 // highest index the leader itself has fsynced (group commit)

	pendingSnapshot *ApplyMsg
	triggers        map[string]chan struct{}
	flushCh         chan struct{}
}

// New creates a node, restoring state from cfg.Storage, and starts its
// background goroutines. Call Stop to shut it down.
func New(cfg Config) (*Node, error) {
	cfg.defaults()
	n := &Node{
		cfg:      cfg,
		id:       cfg.ID,
		peers:    append([]string(nil), cfg.Peers...),
		log:      cfg.Logger.With("node", cfg.ID),
		applyCh:  cfg.ApplyCh,
		done:     make(chan struct{}),
		rng:      rand.New(rand.NewSource(cryptoSeed())),
		entries:  []Entry{{}},
		triggers: map[string]chan struct{}{},
		flushCh:  make(chan struct{}, 1),
	}
	n.applyCond = sync.NewCond(&n.mu)
	st, snap, ok, err := cfg.Storage.Load()
	if err != nil {
		return nil, err
	}
	if ok {
		n.term, n.votedFor, n.entries = st.Term, st.VotedFor, st.Log
		if len(n.entries) == 0 {
			n.entries = []Entry{{}}
		}
		if base := n.entries[0].Index; base > 0 {
			n.snapshot = snap
			n.commitIndex, n.lastApplied = base, base
			n.pendingSnapshot = &ApplyMsg{
				SnapshotValid: true, Snapshot: snap, SnapshotIndex: base, SnapshotTerm: n.entries[0].Term,
			}
		}
	}
	n.persisted = n.lastIndex() // loaded from storage, so durable
	n.resetElectionTimer()
	for _, p := range n.peers {
		n.triggers[p] = make(chan struct{}, 1)
	}

	n.wg.Add(3 + len(n.peers))
	go n.ticker()
	go n.applier()
	go n.flusher()
	for _, p := range n.peers {
		go n.replicator(p)
	}
	return n, nil
}

// Stop shuts the node down. It does not wait for in-flight RPC handlers.
func (n *Node) Stop() {
	if n.stopped.Swap(true) {
		return
	}
	close(n.done)
	n.mu.Lock()
	n.applyCond.Broadcast()
	n.mu.Unlock()
	n.wg.Wait()
}

// ---------------------------------------------------------------------------
// Log helpers (caller holds mu).

func (n *Node) snapIndex() uint64 { return n.entries[0].Index }
func (n *Node) lastIndex() uint64 { return n.entries[len(n.entries)-1].Index }
func (n *Node) lastTerm() uint64  { return n.entries[len(n.entries)-1].Term }

// termAt returns the term of entry i; ok=false if i is compacted or missing.
func (n *Node) termAt(i uint64) (uint64, bool) {
	base := n.snapIndex()
	if i < base || i > n.lastIndex() {
		return 0, false
	}
	return n.entries[i-base].Term, true
}

func (n *Node) slice(from, to uint64) []Entry { // [from, to)
	base := n.snapIndex()
	return append([]Entry(nil), n.entries[from-base:to-base]...)
}

func (n *Node) quorum() int { return (len(n.peers)+1)/2 + 1 }

func (n *Node) persist(snapshot []byte) {
	st := PersistentState{Term: n.term, VotedFor: n.votedFor, Log: n.entries}
	if err := n.cfg.Storage.Save(st, snapshot); err != nil {
		// Losing durability silently would break safety; crash loudly.
		panic("raft: persist failed: " + err.Error())
	}
	// The whole log was just written, so everything in it is durable.
	n.persisted = n.lastIndex()
}

func (n *Node) resetElectionTimer() {
	span := n.cfg.ElectionTimeoutMax - n.cfg.ElectionTimeoutMin
	n.deadline = time.Now().Add(n.cfg.ElectionTimeoutMin + time.Duration(n.rng.Int63n(int64(span))))
}

func (n *Node) becomeFollower(term uint64) {
	if term > n.term {
		n.term = term
		n.votedFor = ""
	}
	n.state = Follower
}

// ---------------------------------------------------------------------------
// Public API.

// Start proposes command for replication. It returns immediately; the
// command is committed if and when it appears on the apply channel at index.
func (n *Node) Start(command []byte) (index, term uint64, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state != Leader || n.stopped.Load() {
		return 0, n.term, false
	}
	e := Entry{Index: n.lastIndex() + 1, Term: n.term, Command: command}
	n.entries = append(n.entries, e)
	n.triggerAll()  // replicate now; followers fsync in parallel with us
	n.signalFlush() // local fsync, batched with other concurrent Starts
	return e.Index, e.Term, true
}

// State returns the current term and whether this node believes it is leader.
func (n *Node) State() (term uint64, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.term, n.state == Leader
}

// Leader returns the ID of the last known leader ("" if unknown).
func (n *Node) Leader() string {
	if n.stopped.Load() {
		return ""
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leader
}

// Status returns a snapshot of internal state.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{
		ID: n.id, State: n.state.String(), Term: n.term, Leader: n.leader,
		CommitIndex: n.commitIndex, LastApplied: n.lastApplied, LastLogIndex: n.lastIndex(),
		SnapshotIndex: n.snapIndex(), LogEntries: len(n.entries) - 1,
	}
}

// Snapshot tells Raft the service has captured its state up to and
// including index, so the log up to index can be discarded.
func (n *Node) Snapshot(index uint64, data []byte) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if index <= n.snapIndex() || index > n.lastApplied {
		return
	}
	t, _ := n.termAt(index)
	rest := n.slice(index+1, n.lastIndex()+1)
	n.entries = append([]Entry{{Index: index, Term: t}}, rest...)
	n.snapshot = data
	n.persist(data)
}

// ReadIndex implements linearizable reads without appending to the log
// (Raft thesis §6.4): note the commit index, confirm we are still leader with
// a round of heartbeats acknowledged by a majority, and return the index.
// The caller must wait until its state machine has applied that index.
func (n *Node) ReadIndex(ctx context.Context) (uint64, error) {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return 0, ErrNotLeader
	}
	// Until the leader commits an entry of its own term (the no-op), its
	// commitIndex may be behind what earlier leaders committed.
	if t, _ := n.termAt(n.commitIndex); t != n.term {
		n.mu.Unlock()
		return 0, ErrNotReady
	}
	readIndex, term := n.commitIndex, n.term
	args := make(map[string]*AppendEntriesArgs, len(n.peers))
	for _, p := range n.peers {
		prev := n.nextIndex[p] - 1
		if prev < n.snapIndex() {
			prev = n.snapIndex()
		}
		pt, _ := n.termAt(prev)
		args[p] = &AppendEntriesArgs{Term: term, LeaderID: n.id, PrevLogIndex: prev, PrevLogTerm: pt, LeaderCommit: n.commitIndex}
	}
	n.mu.Unlock()

	acks := make(chan bool, len(n.peers))
	for p, a := range args {
		go func(p string, a *AppendEntriesArgs) {
			rctx, cancel := context.WithTimeout(ctx, n.cfg.RPCTimeout)
			defer cancel()
			reply, err := n.cfg.Transport.AppendEntries(rctx, p, a)
			if err != nil {
				acks <- false
				return
			}
			n.mu.Lock()
			if reply.Term > n.term {
				n.becomeFollower(reply.Term)
				n.persist(nil)
			}
			n.mu.Unlock()
			// Any reply in our term (even a log mismatch) acknowledges us.
			acks <- reply.Term == term
		}(p, a)
	}
	got := 1 // ourselves
	for i := 0; i < len(n.peers) && got < n.quorum(); i++ {
		select {
		case ok := <-acks:
			if ok {
				got++
			}
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	if got < n.quorum() {
		return 0, ErrNotLeader
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.term != term || n.state != Leader {
		return 0, ErrNotLeader
	}
	return readIndex, nil
}

// ---------------------------------------------------------------------------
// Background loops.

func (n *Node) ticker() {
	defer n.wg.Done()
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-n.done:
			return
		case <-t.C:
		}
		n.mu.Lock()
		if n.state != Leader && time.Now().After(n.deadline) {
			n.startElection()
		}
		n.mu.Unlock()
	}
}

func (n *Node) startElection() {
	n.state = Candidate
	n.term++
	n.votedFor = n.id
	n.leader = ""
	n.persist(nil)
	n.resetElectionTimer()
	term := n.term
	n.log.Debug("starting election", "term", term)
	if n.quorum() == 1 {
		n.becomeLeader()
		return
	}
	args := &RequestVoteArgs{Term: term, CandidateID: n.id, LastLogIndex: n.lastIndex(), LastLogTerm: n.lastTerm()}
	votes := 1
	for _, p := range n.peers {
		go func(p string) {
			ctx, cancel := context.WithTimeout(context.Background(), n.cfg.RPCTimeout)
			defer cancel()
			reply, err := n.cfg.Transport.RequestVote(ctx, p, args)
			if err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if reply.Term > n.term {
				n.becomeFollower(reply.Term)
				n.persist(nil)
				return
			}
			if n.state != Candidate || n.term != term || !reply.VoteGranted {
				return
			}
			votes++
			if votes >= n.quorum() {
				n.becomeLeader()
			}
		}(p)
	}
}

func (n *Node) becomeLeader() {
	n.state = Leader
	n.leader = n.id
	n.nextIndex = map[string]uint64{}
	n.matchIndex = map[string]uint64{}
	for _, p := range n.peers {
		n.nextIndex[p] = n.lastIndex() + 1
		n.matchIndex[p] = 0
	}
	// No-op so entries from previous terms can be committed (§5.4.2) and so
	// ReadIndex becomes available quickly.
	n.entries = append(n.entries, Entry{Index: n.lastIndex() + 1, Term: n.term})
	n.log.Info("became leader", "term", n.term, "lastIndex", n.lastIndex())
	n.signalFlush()
}

func (n *Node) signalFlush() {
	select {
	case n.flushCh <- struct{}{}:
	default:
	}
}

func (n *Node) triggerAll() {
	for _, ch := range n.triggers {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// flusher implements group commit on the leader: entries appended by Start
// are fsynced in batches, and the leader counts itself towards a majority
// only for entries it has persisted. Replication to followers starts at once
// and does not wait for the local fsync.
func (n *Node) flusher() {
	defer n.wg.Done()
	for {
		select {
		case <-n.done:
			return
		case <-n.flushCh:
		}
		n.mu.Lock()
		if n.state == Leader && n.persisted < n.lastIndex() {
			n.triggerAll()
			n.persist(nil)
			n.advanceCommit()
		}
		n.mu.Unlock()
	}
}

func (n *Node) replicator(peer string) {
	defer n.wg.Done()
	hb := time.NewTicker(n.cfg.HeartbeatInterval)
	defer hb.Stop()
	for {
		select {
		case <-n.done:
			return
		case <-hb.C:
		case <-n.triggers[peer]:
		}
		for n.replicateOnce(peer) {
			if n.stopped.Load() {
				return
			}
		}
	}
}

// replicateOnce sends one AppendEntries or InstallSnapshot to peer. It
// returns true if more data should be sent right away.
func (n *Node) replicateOnce(peer string) bool {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return false
	}
	term := n.term
	next := n.nextIndex[peer]
	if next <= n.snapIndex() {
		args := &InstallSnapshotArgs{
			Term: term, LeaderID: n.id, LastIncludedIndex: n.snapIndex(), LastIncludedTerm: n.entries[0].Term,
			Data: n.snapshot,
		}
		n.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 5*n.cfg.RPCTimeout)
		reply, err := n.cfg.Transport.InstallSnapshot(ctx, peer, args)
		cancel()
		if err != nil {
			return false
		}
		n.mu.Lock()
		defer n.mu.Unlock()
		if reply.Term > n.term {
			n.becomeFollower(reply.Term)
			n.persist(nil)
			return false
		}
		if n.state != Leader || n.term != term {
			return false
		}
		if args.LastIncludedIndex > n.matchIndex[peer] {
			n.matchIndex[peer] = args.LastIncludedIndex
		}
		n.nextIndex[peer] = n.matchIndex[peer] + 1
		n.advanceCommit()
		return n.nextIndex[peer] <= n.lastIndex()
	}

	prev := next - 1
	prevTerm, _ := n.termAt(prev)
	end := n.lastIndex() + 1
	if max := next + uint64(n.cfg.MaxEntriesPerRPC); end > max {
		end = max
	}
	args := &AppendEntriesArgs{
		Term: term, LeaderID: n.id, PrevLogIndex: prev, PrevLogTerm: prevTerm,
		Entries: n.slice(next, end), LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.RPCTimeout)
	reply, err := n.cfg.Transport.AppendEntries(ctx, peer, args)
	cancel()
	if err != nil {
		return false
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if reply.Term > n.term {
		n.becomeFollower(reply.Term)
		n.persist(nil)
		return false
	}
	if n.state != Leader || n.term != term {
		return false
	}
	if reply.Success {
		match := prev + uint64(len(args.Entries))
		if match > n.matchIndex[peer] {
			n.matchIndex[peer] = match
		}
		n.nextIndex[peer] = n.matchIndex[peer] + 1
		n.advanceCommit()
	} else {
		n.nextIndex[peer] = n.backtrack(reply)
	}
	return n.nextIndex[peer] <= n.lastIndex()
}

// backtrack computes the next index to try after a rejected AppendEntries
// using the follower's conflict hint (§5.3 optimisation).
func (n *Node) backtrack(r *AppendEntriesReply) uint64 {
	next := r.ConflictIndex
	if r.ConflictTerm != 0 {
		// If we have entries of ConflictTerm, resume after our last one.
		for i := n.lastIndex(); i > n.snapIndex(); i-- {
			if t, _ := n.termAt(i); t == r.ConflictTerm {
				next = i + 1
				break
			} else if t < r.ConflictTerm {
				break
			}
		}
	}
	if next < 1 {
		next = 1
	}
	return next
}

// advanceCommit commits the highest index replicated on a majority, but only
// if that entry is from the current term (Figure 8 safety).
func (n *Node) advanceCommit() {
	if n.state != Leader {
		return
	}
	matches := []uint64{n.persisted}
	for _, p := range n.peers {
		matches = append(matches, n.matchIndex[p])
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	candidate := matches[n.quorum()-1]
	if candidate > n.commitIndex {
		if t, ok := n.termAt(candidate); ok && t == n.term {
			n.commitIndex = candidate
			n.applyCond.Broadcast()
			n.triggerAll() // propagate the new commit index promptly
		}
	}
}

func (n *Node) applier() {
	defer n.wg.Done()
	for {
		n.mu.Lock()
		for !n.stopped.Load() && n.pendingSnapshot == nil && n.lastApplied >= n.commitIndex {
			n.applyCond.Wait()
		}
		if n.stopped.Load() {
			n.mu.Unlock()
			return
		}
		var msgs []ApplyMsg
		if n.pendingSnapshot != nil {
			msgs = append(msgs, *n.pendingSnapshot)
			if n.pendingSnapshot.SnapshotIndex > n.lastApplied {
				n.lastApplied = n.pendingSnapshot.SnapshotIndex
			}
			n.pendingSnapshot = nil
		}
		if n.lastApplied < n.commitIndex {
			from := n.lastApplied + 1
			if from <= n.snapIndex() {
				from = n.snapIndex() + 1
			}
			for _, e := range n.slice(from, n.commitIndex+1) {
				msgs = append(msgs, ApplyMsg{CommandValid: true, Command: e.Command, Index: e.Index, Term: e.Term})
			}
			n.lastApplied = n.commitIndex
		}
		n.mu.Unlock()
		for _, m := range msgs {
			select {
			case n.applyCh <- m:
			case <-n.done:
				return
			}
		}
	}
}

// ---------------------------------------------------------------------------
// RPC handlers.

// HandleRequestVote implements Handler.
func (n *Node) HandleRequestVote(args *RequestVoteArgs) (*RequestVoteReply, error) {
	if n.stopped.Load() {
		return nil, ErrStopped
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if args.Term < n.term {
		return &RequestVoteReply{Term: n.term}, nil
	}
	changed := false
	if args.Term > n.term {
		n.becomeFollower(args.Term)
		changed = true
	}
	upToDate := args.LastLogTerm > n.lastTerm() ||
		(args.LastLogTerm == n.lastTerm() && args.LastLogIndex >= n.lastIndex())
	granted := false
	if (n.votedFor == "" || n.votedFor == args.CandidateID) && upToDate {
		n.votedFor = args.CandidateID
		granted, changed = true, true
		n.resetElectionTimer()
	}
	if changed {
		n.persist(nil)
	}
	return &RequestVoteReply{Term: n.term, VoteGranted: granted}, nil
}

// HandleAppendEntries implements Handler.
func (n *Node) HandleAppendEntries(args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	if n.stopped.Load() {
		return nil, ErrStopped
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if args.Term < n.term {
		return &AppendEntriesReply{Term: n.term}, nil
	}
	dirty := args.Term > n.term
	n.becomeFollower(args.Term)
	n.leader = args.LeaderID
	n.resetElectionTimer()

	prev, prevTerm, entries := args.PrevLogIndex, args.PrevLogTerm, args.Entries
	if prev < n.snapIndex() {
		// The prefix is already in our snapshot; skip covered entries.
		skip := n.snapIndex() - prev
		if skip >= uint64(len(entries)) {
			entries = nil
		} else {
			entries = entries[skip:]
		}
		prev, prevTerm = n.snapIndex(), n.entries[0].Term
	}
	reply := &AppendEntriesReply{Term: n.term}
	if prev > n.lastIndex() {
		reply.ConflictIndex = n.lastIndex() + 1
		if dirty {
			n.persist(nil)
		}
		return reply, nil
	}
	if t, _ := n.termAt(prev); t != prevTerm {
		reply.ConflictTerm = t
		i := prev
		for i > n.snapIndex()+1 {
			if pt, _ := n.termAt(i - 1); pt != t {
				break
			}
			i--
		}
		reply.ConflictIndex = i
		if dirty {
			n.persist(nil)
		}
		return reply, nil
	}

	for j, e := range entries {
		if e.Index <= n.lastIndex() {
			if t, _ := n.termAt(e.Index); t == e.Term {
				continue
			}
			// Conflict: delete it and everything after (§5.3).
			n.entries = n.entries[:e.Index-n.snapIndex()]
		}
		n.entries = append(n.entries, entries[j:]...)
		dirty = true
		break
	}
	if dirty {
		n.persist(nil)
	}
	if args.LeaderCommit > n.commitIndex {
		lastNew := prev + uint64(len(entries))
		c := args.LeaderCommit
		if lastNew < c {
			c = lastNew
		}
		if c > n.commitIndex {
			n.commitIndex = c
			n.applyCond.Broadcast()
		}
	}
	reply.Success = true
	return reply, nil
}

// HandleInstallSnapshot implements Handler.
func (n *Node) HandleInstallSnapshot(args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	if n.stopped.Load() {
		return nil, ErrStopped
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if args.Term < n.term {
		return &InstallSnapshotReply{Term: n.term}, nil
	}
	n.becomeFollower(args.Term)
	n.leader = args.LeaderID
	n.resetElectionTimer()
	if args.LastIncludedIndex <= n.commitIndex {
		n.persist(nil)
		return &InstallSnapshotReply{Term: n.term}, nil
	}
	var rest []Entry
	if t, ok := n.termAt(args.LastIncludedIndex); ok && t == args.LastIncludedTerm {
		rest = n.slice(args.LastIncludedIndex+1, n.lastIndex()+1)
	}
	n.entries = append([]Entry{{Index: args.LastIncludedIndex, Term: args.LastIncludedTerm}}, rest...)
	n.snapshot = args.Data
	n.commitIndex = args.LastIncludedIndex
	n.persist(args.Data)
	n.pendingSnapshot = &ApplyMsg{
		SnapshotValid: true, Snapshot: args.Data,
		SnapshotIndex: args.LastIncludedIndex, SnapshotTerm: args.LastIncludedTerm,
	}
	n.applyCond.Broadcast()
	return &InstallSnapshotReply{Term: n.term}, nil
}

// cryptoSeed seeds each node's election-timeout RNG independently. Seeding
// from the clock is a classic trap: nodes started in the same tick (easy on
// Windows' coarse clock) get identical "random" timeouts and split every vote
// forever. TestElectionTimeoutsDiffer guards against this.
func cryptoSeed() int64 {
	var b [8]byte
	_, _ = crand.Read(b[:])
	return int64(binary.LittleEndian.Uint64(b[:]))
}
