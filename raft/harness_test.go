package raft

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

// cluster is a test harness: n nodes on a MemNetwork, each with an applier
// that checks State Machine Safety on every applied entry: no two nodes may
// apply different commands at the same index.
type cluster struct {
	t        *testing.T
	net      *MemNetwork
	ids      []string
	nodes    []*Node
	storages []*MemoryStorage
	snapEach int // service snapshots every snapEach applied entries (0 = never)

	mu      sync.Mutex
	logs    []map[uint64]string // per node: index -> command
	applied []uint64
	errMsg  string
}

func newCluster(t *testing.T, n int, reliable bool, snapEach int) *cluster {
	t.Helper()
	c := &cluster{t: t, net: NewMemNetwork(), snapEach: snapEach}
	c.net.SetReliable(reliable)
	for i := 0; i < n; i++ {
		c.ids = append(c.ids, fmt.Sprintf("n%d", i))
	}
	c.nodes = make([]*Node, n)
	c.storages = make([]*MemoryStorage, n)
	c.logs = make([]map[uint64]string, n)
	c.applied = make([]uint64, n)
	for i := 0; i < n; i++ {
		c.storages[i] = NewMemoryStorage()
		c.start(i)
	}
	t.Cleanup(c.cleanup)
	return c
}

func (c *cluster) peersOf(i int) []string {
	var out []string
	for j, id := range c.ids {
		if j != i {
			out = append(out, id)
		}
	}
	return out
}

// start (re)starts node i from its storage.
func (c *cluster) start(i int) {
	c.crash(i)
	c.mu.Lock()
	c.logs[i] = map[uint64]string{}
	c.applied[i] = 0
	c.mu.Unlock()
	ch := make(chan ApplyMsg, 64)
	n, err := New(Config{
		ID: c.ids[i], Peers: c.peersOf(i), Storage: c.storages[i], Transport: c.net.Transport(c.ids[i]),
		ApplyCh: ch, ElectionTimeoutMin: 150 * time.Millisecond, ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval: 40 * time.Millisecond, RPCTimeout: 120 * time.Millisecond,
	})
	if err != nil {
		c.t.Fatal(err)
	}
	c.nodes[i] = n
	c.net.Register(c.ids[i], n)
	go c.applier(i, n, ch)
}

// crash stops node i and gives it a copy of its storage, so goroutines still
// running in the dead instance can't affect the restarted one.
func (c *cluster) crash(i int) {
	if n := c.nodes[i]; n != nil {
		c.net.Disconnect(c.ids[i])
		n.Stop()
		c.nodes[i] = nil
		c.storages[i] = c.storages[i].Clone()
	}
}

func (c *cluster) applier(i int, n *Node, ch chan ApplyMsg) {
	for {
		var m ApplyMsg
		select {
		case m = <-ch:
		case <-n.done:
			return
		}
		c.mu.Lock()
		switch {
		case m.SnapshotValid:
			var snap map[uint64]string
			if err := json.Unmarshal(m.Snapshot, &snap); err != nil {
				c.fail("node %d: bad snapshot: %v", i, err)
			}
			if m.SnapshotIndex > c.applied[i] {
				c.logs[i] = snap
				c.applied[i] = m.SnapshotIndex
			}
		case m.CommandValid:
			if m.Index <= c.applied[i] {
				break // already covered by a snapshot
			}
			if m.Index != c.applied[i]+1 {
				c.fail("node %d applied index %d out of order (last %d)", i, m.Index, c.applied[i])
			}
			val := string(m.Command)
			if m.Command == nil {
				val = "<noop>"
			}
			for j := range c.logs {
				if old, ok := c.logs[j][m.Index]; ok && old != val {
					c.fail("SAFETY: index %d applied %q on node %d but %q on node %d", m.Index, val, i, old, j)
				}
			}
			c.logs[i][m.Index] = val
			c.applied[i] = m.Index
			if c.snapEach > 0 && m.Index%uint64(c.snapEach) == 0 {
				data, _ := json.Marshal(c.logs[i])
				idx := m.Index
				c.mu.Unlock()
				n.Snapshot(idx, data)
				continue
			}
		}
		c.mu.Unlock()
	}
}

func (c *cluster) fail(format string, args ...any) {
	if c.errMsg == "" {
		c.errMsg = fmt.Sprintf(format, args...)
	}
}

func (c *cluster) checkErr() {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.errMsg != "" {
		c.t.Fatal(c.errMsg)
	}
}

func (c *cluster) cleanup() {
	for i := range c.nodes {
		c.crash(i)
	}
	c.checkErr()
}

func (c *cluster) connect(i int)    { c.net.Connect(c.ids[i]) }
func (c *cluster) disconnect(i int) { c.net.Disconnect(c.ids[i]) }

func (c *cluster) isConnected(i int) bool {
	c.net.mu.Lock()
	defer c.net.mu.Unlock()
	return c.net.up[c.ids[i]]
}

// checkOneLeader waits for exactly one leader among connected nodes.
func (c *cluster) checkOneLeader() int {
	c.t.Helper()
	for iter := 0; iter < 10; iter++ {
		time.Sleep(time.Duration(450+rand.Intn(100)) * time.Millisecond)
		leaders := map[uint64][]int{}
		for i, n := range c.nodes {
			if n != nil && c.isConnected(i) {
				if term, ok := n.State(); ok {
					leaders[term] = append(leaders[term], i)
				}
			}
		}
		var last uint64
		for term, ls := range leaders {
			if len(ls) > 1 {
				c.t.Fatalf("term %d has %d leaders", term, len(ls))
			}
			if term > last {
				last = term
			}
		}
		if len(leaders) > 0 {
			return leaders[last][0]
		}
	}
	c.t.Fatal("expected one leader, got none")
	return -1
}

func (c *cluster) checkNoLeader() {
	c.t.Helper()
	for i, n := range c.nodes {
		if n != nil && c.isConnected(i) {
			if _, ok := n.State(); ok {
				c.t.Fatalf("node %d claims to be leader but should not", i)
			}
		}
	}
}

func (c *cluster) checkTerms() uint64 {
	c.t.Helper()
	var term uint64
	for i, n := range c.nodes {
		if n != nil && c.isConnected(i) {
			t, _ := n.State()
			if term == 0 {
				term = t
			} else if t != term {
				c.t.Fatalf("nodes disagree on term: %d vs %d", term, t)
			}
		}
	}
	return term
}

// nCommitted reports how many nodes have applied index and the command.
func (c *cluster) nCommitted(index uint64) (int, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	count, cmd := 0, ""
	for i := range c.logs {
		if v, ok := c.logs[i][index]; ok {
			if count > 0 && v != cmd {
				c.fail("committed values differ at %d: %q vs %q", index, cmd, v)
			}
			count, cmd = count+1, v
		}
	}
	return count, cmd
}

// one submits cmd to the leader and waits until at least expected nodes
// apply it, retrying on leader changes for up to 10s. Returns the index.
func (c *cluster) one(cmd string, expected int, retry bool) uint64 {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	start := 0
	for time.Now().Before(deadline) {
		var index uint64
		for k := 0; k < len(c.nodes); k++ {
			i := (start + k) % len(c.nodes)
			n := c.nodes[i]
			if n == nil || !c.isConnected(i) {
				continue
			}
			if idx, _, ok := n.Start([]byte(cmd)); ok {
				index = idx
				start = i
				break
			}
		}
		if index != 0 {
			t1 := time.Now()
			for time.Since(t1) < 2*time.Second {
				if nd, v := c.nCommitted(index); nd >= expected && v == cmd {
					return index
				}
				time.Sleep(20 * time.Millisecond)
			}
			if !retry {
				c.t.Fatalf("one(%q) failed to reach agreement", cmd)
			}
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	c.checkErr()
	c.t.Fatalf("one(%q) failed to reach agreement in 10s", cmd)
	return 0
}

func cmd(i int) string { return "cmd-" + strconv.Itoa(i) }
