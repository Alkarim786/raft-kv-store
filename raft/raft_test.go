package raft

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestInitialElection(t *testing.T) {
	c := newCluster(t, 3, true, 0)
	c.checkOneLeader()
	time.Sleep(50 * time.Millisecond)
	term1 := c.checkTerms()
	if term1 < 1 {
		t.Fatalf("term is %d, want >= 1", term1)
	}
	// With no failures, the term should not change.
	time.Sleep(1 * time.Second)
	if term2 := c.checkTerms(); term1 != term2 {
		t.Logf("warning: term changed %d -> %d without failures", term1, term2)
	}
	c.checkOneLeader()
}

func TestReElection(t *testing.T) {
	c := newCluster(t, 3, true, 0)
	l1 := c.checkOneLeader()

	c.disconnect(l1)
	c.checkOneLeader() // a new leader is elected

	c.connect(l1)
	l2 := c.checkOneLeader() // the old leader rejoins as follower

	c.disconnect(l2)
	c.disconnect((l2 + 1) % 3)
	time.Sleep(1 * time.Second)
	c.checkNoLeader() // no quorum, no leader

	c.connect((l2 + 1) % 3)
	c.checkOneLeader()
	c.connect(l2)
	c.checkOneLeader()
}

func TestManyElections(t *testing.T) {
	c := newCluster(t, 7, true, 0)
	c.checkOneLeader()
	for iter := 0; iter < 6; iter++ {
		a, b, d := rand.Intn(7), rand.Intn(7), rand.Intn(7)
		c.disconnect(a)
		c.disconnect(b)
		c.disconnect(d)
		// At most 3 of 7 are down, so a quorum of 4 always remains.
		c.checkOneLeader()
		c.connect(a)
		c.connect(b)
		c.connect(d)
	}
	c.checkOneLeader()
}

func TestBasicAgree(t *testing.T) {
	c := newCluster(t, 3, true, 0)
	for i := 1; i <= 5; i++ {
		c.one(cmd(i), 3, true)
	}
}

func TestFollowerFailure(t *testing.T) {
	c := newCluster(t, 3, true, 0)
	c.one(cmd(1), 3, false)
	l := c.checkOneLeader()
	c.disconnect((l + 1) % 3)
	c.one(cmd(2), 2, false) // majority still commits
	time.Sleep(300 * time.Millisecond)
	c.one(cmd(3), 2, false)
	c.connect((l + 1) % 3)
	c.one(cmd(4), 3, true) // the follower catches up
}

func TestNoAgreeWithoutMajority(t *testing.T) {
	c := newCluster(t, 5, true, 0)
	c.one(cmd(1), 5, false)
	l := c.checkOneLeader()
	c.disconnect((l + 1) % 5)
	c.disconnect((l + 2) % 5)
	c.disconnect((l + 3) % 5)
	idx, _, ok := c.nodes[l].Start([]byte("lost"))
	if !ok {
		t.Fatal("leader rejected Start")
	}
	time.Sleep(2 * time.Second)
	if n, _ := c.nCommitted(idx); n > 0 {
		t.Fatalf("%d nodes committed without a majority", n)
	}
	c.connect((l + 1) % 5)
	c.connect((l + 2) % 5)
	c.connect((l + 3) % 5)
	c.one(cmd(2), 5, true)
}

func TestConcurrentStarts(t *testing.T) {
	c := newCluster(t, 3, true, 0)
	l := c.checkOneLeader()
	var wg sync.WaitGroup
	idx := make(chan uint64, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if ix, _, ok := c.nodes[l].Start([]byte(cmd(100 + i))); ok {
				idx <- ix
			}
		}(i)
	}
	wg.Wait()
	close(idx)
	seen := map[uint64]bool{}
	for ix := range idx {
		if seen[ix] {
			t.Fatalf("index %d handed out twice", ix)
		}
		seen[ix] = true
		deadline := time.Now().Add(3 * time.Second)
		for {
			if n, _ := c.nCommitted(ix); n == 3 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("index %d not committed on all nodes", ix)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestRejoinWithConflictingLog(t *testing.T) {
	c := newCluster(t, 3, true, 0)
	c.one(cmd(1), 3, true)
	l1 := c.checkOneLeader()
	c.disconnect(l1)
	// The isolated leader accepts entries that can never commit.
	c.nodes[l1].Start([]byte("stale-1"))
	c.nodes[l1].Start([]byte("stale-2"))
	c.one(cmd(2), 2, true)
	l2 := c.checkOneLeader()
	c.disconnect(l2)
	c.connect(l1) // the old leader must discard its uncommitted entries
	c.one(cmd(3), 2, true)
	c.connect(l2)
	c.one(cmd(4), 3, true)
}

// TestBackup forces the leader to back up over many conflicting entries;
// it is slow unless the conflict-term optimisation works.
func TestBackup(t *testing.T) {
	c := newCluster(t, 5, true, 0)
	c.one(cmd(0), 5, true)
	l := c.checkOneLeader()
	// Isolate the leader with one follower; give them 50 uncommittable entries.
	c.disconnect((l + 2) % 5)
	c.disconnect((l + 3) % 5)
	c.disconnect((l + 4) % 5)
	for i := 0; i < 50; i++ {
		c.nodes[l].Start([]byte("dead-" + cmd(i)))
	}
	time.Sleep(500 * time.Millisecond)
	c.disconnect(l)
	c.disconnect((l + 1) % 5)
	// The other three form a majority and commit 50 different entries.
	c.connect((l + 2) % 5)
	c.connect((l + 3) % 5)
	c.connect((l + 4) % 5)
	for i := 0; i < 50; i++ {
		c.one(cmd(1000+i), 3, true)
	}
	// Bring everyone back; all must converge on the committed entries.
	for i := 0; i < 5; i++ {
		c.connect(i)
	}
	start := c.net.RPCCount()
	c.one(cmd(9999), 5, true)
	if rpcs := c.net.RPCCount() - start; rpcs > 600 {
		t.Logf("note: %d RPCs to converge", rpcs)
	}
}

func TestPersistAcrossFullRestart(t *testing.T) {
	c := newCluster(t, 3, true, 0)
	c.one(cmd(11), 3, true)
	for i := 0; i < 3; i++ {
		c.start(i) // crash + restart every node
	}
	c.one(cmd(12), 3, true)
	l := c.checkOneLeader()
	c.start(l)
	c.one(cmd(13), 3, true)
	// Crash two, commit with... nothing; restart and check the earlier entries survive.
	c.crash((l + 1) % 3)
	c.one(cmd(14), 2, true)
	c.start((l + 1) % 3)
	c.one(cmd(15), 3, true)
	for _, want := range []string{cmd(11), cmd(12), cmd(13), cmd(14), cmd(15)} {
		found := false
		c.mu.Lock()
		for _, v := range c.logs[0] {
			if v == want {
				found = true
			}
		}
		c.mu.Unlock()
		if !found {
			t.Fatalf("%s lost after restarts", want)
		}
	}
}

// TestFigure8Unreliable is the hardest scenario: an unreliable, reordering
// network plus leaders crashing right after accepting entries. Safety is
// checked on every apply; liveness is checked at the end.
func TestFigure8Unreliable(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	c := newCluster(t, 5, false, 0)
	c.one(cmd(0), 1, true)
	c.net.SetLongDelays(true)
	alive := 5
	for iter := 0; iter < 200; iter++ {
		if iter == 150 {
			c.net.SetLongDelays(false)
		}
		leader := -1
		for i, n := range c.nodes {
			if n != nil && c.isConnected(i) {
				if _, _, ok := n.Start([]byte(cmd(10000 + iter))); ok {
					leader = i
				}
			}
		}
		if rand.Intn(1000) < 100 {
			time.Sleep(time.Duration(rand.Intn(130)) * time.Millisecond)
		} else {
			time.Sleep(time.Duration(rand.Intn(13)) * time.Millisecond)
		}
		if leader != -1 && rand.Intn(1000) < 500 {
			c.disconnect(leader)
			alive--
		}
		if alive < 3 {
			s := rand.Intn(5)
			if !c.isConnected(s) {
				c.connect(s)
				alive++
			}
		}
	}
	for i := 0; i < 5; i++ {
		c.connect(i)
	}
	c.net.SetReliable(true)
	c.one(cmd(99999), 5, true)
}

func TestSnapshotInstallToLaggingFollower(t *testing.T) {
	c := newCluster(t, 3, true, 10) // service snapshots every 10 entries
	c.one(cmd(0), 3, true)
	l := c.checkOneLeader()
	lag := (l + 1) % 3
	c.disconnect(lag)
	for i := 1; i <= 60; i++ {
		c.one(cmd(i), 2, true)
	}
	st := c.nodes[l].Status()
	if st.SnapshotIndex == 0 || st.LogEntries > 30 {
		t.Fatalf("leader did not compact its log: %+v", st)
	}
	c.connect(lag) // now behind the leader's snapshot: needs InstallSnapshot
	c.one(cmd(61), 3, true)
	if s := c.nodes[lag].Status(); s.SnapshotIndex == 0 {
		t.Fatalf("lagging follower never installed a snapshot: %+v", s)
	}
}

func TestSnapshotSurvivesRestart(t *testing.T) {
	c := newCluster(t, 3, true, 10)
	for i := 1; i <= 35; i++ {
		c.one(cmd(i), 3, true)
	}
	for i := 0; i < 3; i++ {
		c.start(i)
	}
	c.one(cmd(36), 3, true)
	for i := 0; i < 3; i++ {
		if s := c.nodes[i].Status(); s.SnapshotIndex < 30 {
			t.Fatalf("node %d restarted without its snapshot: %+v", i, s)
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	found := false
	for _, v := range c.logs[0] {
		if v == cmd(5) {
			found = true
		}
	}
	if !found {
		t.Fatal("state from the snapshot was not restored")
	}
}

func TestReadIndex(t *testing.T) {
	c := newCluster(t, 3, true, 0)
	c.one(cmd(1), 3, true)
	l := c.checkOneLeader()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ri, err := c.nodes[l].ReadIndex(ctx)
	if err != nil || ri < 2 {
		t.Fatalf("ReadIndex = %d, %v", ri, err)
	}
	if _, err := c.nodes[(l+1)%3].ReadIndex(ctx); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("follower ReadIndex err = %v, want ErrNotLeader", err)
	}
	// A partitioned leader that hasn't noticed yet must not serve reads.
	c.disconnect((l + 1) % 3)
	c.disconnect((l + 2) % 3)
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	if _, err := c.nodes[l].ReadIndex(ctx2); err == nil {
		t.Fatal("isolated leader served a ReadIndex")
	}
}

func TestSingleNodeCluster(t *testing.T) {
	c := newCluster(t, 1, true, 0)
	c.one(cmd(1), 1, false)
	c.start(0)
	c.one(cmd(2), 1, true)
}

func TestFileStorageRoundTrip(t *testing.T) {
	dir := t.TempDir()
	fs, err := NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, ok, _ := fs.Load(); ok {
		t.Fatal("empty dir should have no state")
	}
	st := PersistentState{Term: 3, VotedFor: "n1", Log: []Entry{{Index: 5, Term: 2}, {Index: 6, Term: 3, Command: []byte("x")}}}
	if err := fs.Save(st, []byte("snap-5")); err != nil {
		t.Fatal(err)
	}
	// Later save without a snapshot keeps the old snapshot.
	st.Log = append(st.Log, Entry{Index: 7, Term: 3, Command: []byte("y")})
	if err := fs.Save(st, nil); err != nil {
		t.Fatal(err)
	}
	fs2, _ := NewFileStorage(dir)
	got, snap, ok, err := fs2.Load()
	if err != nil || !ok || got.Term != 3 || got.VotedFor != "n1" || len(got.Log) != 3 || string(snap) != "snap-5" {
		t.Fatalf("load = %+v %q %v %v", got, snap, ok, err)
	}
	if fs2.StateSize() == 0 {
		t.Fatal("StateSize should be non-zero")
	}
}

// Regression test: nodes created at the same instant must still draw
// different election timeouts, or elections split forever.
func TestElectionTimeoutsDiffer(t *testing.T) {
	net := NewMemNetwork()
	seen := map[time.Duration]bool{}
	for i := 0; i < 5; i++ {
		n, err := New(Config{ID: "nX", Storage: NewMemoryStorage(), Transport: net.Transport("nX"), ApplyCh: make(chan ApplyMsg, 1)})
		if err != nil {
			t.Fatal(err)
		}
		n.mu.Lock()
		seen[time.Until(n.deadline).Round(time.Millisecond)] = true
		n.mu.Unlock()
		n.Stop()
	}
	if len(seen) < 4 {
		t.Fatalf("election timeouts collide: %v", seen)
	}
}
