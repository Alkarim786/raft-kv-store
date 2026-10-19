package kv

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/Alkarim786/raft-kv-store/client"
	"github.com/Alkarim786/raft-kv-store/raft"
)

// kvCluster runs n Stores over an in-memory network.
type kvCluster struct {
	t        *testing.T
	net      *raft.MemNetwork
	ids      []string
	nodes    []*raft.Node
	stores   []*Store
	storages []*raft.MemoryStorage
	maxRaft  int
	mu       sync.Mutex
}

func newKVCluster(t *testing.T, n, maxRaft int) *kvCluster {
	c := &kvCluster{t: t, net: raft.NewMemNetwork(), maxRaft: maxRaft}
	for i := 0; i < n; i++ {
		c.ids = append(c.ids, "n"+strconv.Itoa(i))
	}
	c.nodes = make([]*raft.Node, n)
	c.stores = make([]*Store, n)
	c.storages = make([]*raft.MemoryStorage, n)
	for i := 0; i < n; i++ {
		c.storages[i] = raft.NewMemoryStorage()
		c.start(i)
	}
	t.Cleanup(func() {
		for i := range c.nodes {
			c.crash(i)
		}
	})
	return c
}

func (c *kvCluster) start(i int) {
	c.crash(i)
	var peers []string
	for j, id := range c.ids {
		if j != i {
			peers = append(peers, id)
		}
	}
	ch := make(chan raft.ApplyMsg, 128)
	n, err := raft.New(raft.Config{
		ID: c.ids[i], Peers: peers, Storage: c.storages[i], Transport: c.net.Transport(c.ids[i]), ApplyCh: ch,
		ElectionTimeoutMin: 150 * time.Millisecond, ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval: 40 * time.Millisecond, RPCTimeout: 120 * time.Millisecond,
	})
	if err != nil {
		c.t.Fatal(err)
	}
	c.mu.Lock()
	c.nodes[i] = n
	c.stores[i] = NewStore(n, c.storages[i], ch, c.maxRaft)
	c.mu.Unlock()
	c.net.Register(c.ids[i], n)
}

func (c *kvCluster) crash(i int) {
	c.mu.Lock()
	n, s := c.nodes[i], c.stores[i]
	c.nodes[i], c.stores[i] = nil, nil
	c.mu.Unlock()
	if n != nil {
		c.net.Disconnect(c.ids[i])
		n.Stop()
		s.Stop()
		c.storages[i] = c.storages[i].Clone()
	}
}

func (c *kvCluster) store(i int) *Store {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stores[i]
}

// testClerk talks to Stores directly, trying each until one accepts.
type testClerk struct {
	c      *kvCluster
	id     string
	seq    uint64
	leader int
}

func (c *kvCluster) clerk() *testClerk {
	return &testClerk{c: c, id: fmt.Sprintf("clerk-%d", rand.Int63())}
}

func (k *testClerk) try(fn func(*Store) (Result, error)) (Result, error) {
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		s := k.c.store(k.leader)
		if s != nil {
			res, err := fn(s)
			if err == nil {
				return res, nil
			}
		}
		k.leader = (k.leader + 1) % len(k.c.ids)
		time.Sleep(15 * time.Millisecond)
	}
	return Result{}, errors.New("no leader in 20s")
}

func (k *testClerk) put(key, val string) (Result, error) {
	k.seq++
	op := Op{Kind: OpPut, Key: key, Value: val, ClientID: k.id, Seq: k.seq}
	return k.try(func(s *Store) (Result, error) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		return s.Submit(ctx, op)
	})
}

func (k *testClerk) get(key string) (Result, error) {
	return k.try(func(s *Store) (Result, error) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		return s.Get(ctx, key)
	})
}

func TestPutGetDeleteCAS(t *testing.T) {
	c := newKVCluster(t, 3, 0)
	k := c.clerk()
	if _, err := k.put("a", "1"); err != nil {
		t.Fatal(err)
	}
	if r, _ := k.get("a"); r.Value != "1" || !r.Found {
		t.Fatalf("get a = %+v", r)
	}
	k.seq++
	cas := Op{Kind: OpCAS, Key: "a", Expect: "1", Value: "2", ClientID: k.id, Seq: k.seq}
	r, err := k.try(func(s *Store) (Result, error) { return s.Submit(context.Background(), cas) })
	if err != nil || !r.Applied || r.Value != "2" {
		t.Fatalf("cas = %+v %v", r, err)
	}
	k.seq++
	bad := Op{Kind: OpCAS, Key: "a", Expect: "1", Value: "3", ClientID: k.id, Seq: k.seq}
	if r, _ := k.try(func(s *Store) (Result, error) { return s.Submit(context.Background(), bad) }); r.Applied || r.Value != "2" {
		t.Fatalf("stale cas should fail: %+v", r)
	}
	k.seq++
	del := Op{Kind: OpDelete, Key: "a", ClientID: k.id, Seq: k.seq}
	if _, err := k.try(func(s *Store) (Result, error) { return s.Submit(context.Background(), del) }); err != nil {
		t.Fatal(err)
	}
	if r, _ := k.get("a"); r.Found {
		t.Fatalf("deleted key still present: %+v", r)
	}
}

func TestDuplicateSubmitAppliesOnce(t *testing.T) {
	c := newKVCluster(t, 3, 0)
	k := c.clerk()
	k.put("x", "0")
	// Same (client, seq) submitted twice, e.g. a retry after a lost reply.
	k.seq++
	op := Op{Kind: OpCAS, Key: "x", Expect: "0", Value: "1", ClientID: k.id, Seq: k.seq}
	r1, err := k.try(func(s *Store) (Result, error) { return s.Submit(context.Background(), op) })
	if err != nil {
		t.Fatal(err)
	}
	r2, err := k.try(func(s *Store) (Result, error) { return s.Submit(context.Background(), op) })
	if err != nil {
		t.Fatal(err)
	}
	if !r1.Applied || r1 != r2 {
		t.Fatalf("retry must return the original result: %+v vs %+v", r1, r2)
	}
}

func TestSnapshotsBoundRaftState(t *testing.T) {
	const limit = 8 << 10
	c := newKVCluster(t, 3, limit)
	k := c.clerk()
	for i := 0; i < 400; i++ {
		if _, err := k.put("key"+strconv.Itoa(i%20), fmt.Sprintf("value-%04d-%s", i, "padding-padding")); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(300 * time.Millisecond)
	for i := range c.storages {
		if sz := c.storages[i].StateSize(); sz > 3*limit {
			t.Fatalf("node %d raft state %d bytes, want <= ~%d", i, sz, limit)
		}
	}
	// Restart everyone: state must come back from snapshots + log.
	for i := range c.ids {
		c.start(i)
	}
	if r, err := k.get("key19"); err != nil || r.Value != "value-0399-padding-padding" {
		t.Fatalf("after restart: %+v %v", r, err)
	}
}

// --- Linearizability (porcupine) -------------------------------------------

type kvInput struct {
	put   bool
	key   string
	value string
}

var kvModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		for _, op := range history {
			k := op.Input.(kvInput).key
			byKey[k] = append(byKey[k], op)
		}
		var out [][]porcupine.Operation
		for _, ops := range byKey {
			out = append(out, ops)
		}
		return out
	},
	Init: func() any { return "" },
	Step: func(state, input, output any) (bool, any) {
		in := input.(kvInput)
		if in.put {
			return true, in.value
		}
		return output.(string) == state.(string), state
	},
	Equal: func(a, b any) bool { return a == b },
}

// TestLinearizableUnderPartitionsAndCrashes runs concurrent clients while
// the network partitions and nodes crash and restart, then checks the
// recorded history is linearizable.
func TestLinearizableUnderPartitionsAndCrashes(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	c := newKVCluster(t, 5, 4<<10)
	var mu sync.Mutex
	var history []porcupine.Operation
	var stop atomic.Bool
	var wg sync.WaitGroup
	start := time.Now()
	now := func() int64 { return int64(time.Since(start)) }

	for cl := 0; cl < 6; cl++ {
		wg.Add(1)
		go func(cl int) {
			defer wg.Done()
			k := c.clerk()
			for i := 0; !stop.Load(); i++ {
				key := strconv.Itoa(rand.Intn(3))
				if rand.Intn(2) == 0 {
					val := fmt.Sprintf("c%d-%d", cl, i)
					call := now()
					if _, err := k.put(key, val); err != nil {
						continue // unknown outcome; porcupine handles missing ops conservatively only if recorded
					}
					ret := now()
					mu.Lock()
					history = append(history, porcupine.Operation{ClientId: cl, Input: kvInput{true, key, val}, Call: call, Output: "", Return: ret})
					mu.Unlock()
				} else {
					call := now()
					r, err := k.get(key)
					if err != nil {
						continue
					}
					ret := now()
					mu.Lock()
					history = append(history, porcupine.Operation{ClientId: cl, Input: kvInput{false, key, ""}, Call: call, Output: r.Value, Return: ret})
					mu.Unlock()
				}
			}
		}(cl)
	}

	// Chaos: partitions and crashes for ~8 seconds.
	for round := 0; round < 8; round++ {
		time.Sleep(600 * time.Millisecond)
		perm := rand.Perm(5)
		a, b := []string{}, []string{}
		for j, p := range perm {
			if j < 2 {
				a = append(a, c.ids[p])
			} else {
				b = append(b, c.ids[p])
			}
		}
		c.net.Partition(a, b)
		time.Sleep(400 * time.Millisecond)
		c.net.Partition()
		if round%3 == 2 {
			c.start(rand.Intn(5)) // crash + restart a node
		}
	}
	stop.Store(true)
	wg.Wait()

	if len(history) < 50 {
		t.Fatalf("too few completed operations (%d) — liveness problem", len(history))
	}
	res := porcupine.CheckOperationsTimeout(kvModel, history, 60*time.Second)
	if res == porcupine.Illegal {
		t.Fatalf("history of %d operations is NOT linearizable", len(history))
	}
	t.Logf("%d operations checked: %v", len(history), res)
}

// --- HTTP end to end ---------------------------------------------------------

func TestHTTPEndToEndWithLeaderRedirects(t *testing.T) {
	c := newKVCluster(t, 3, 0)
	servers := make([]*httptest.Server, 3)
	urls := map[string]string{}
	var list []string
	handlers := make([]*http.ServeMux, 3)
	for i := range servers {
		handlers[i] = http.NewServeMux()
		servers[i] = httptest.NewServer(handlers[i])
		t.Cleanup(servers[i].Close)
		urls[c.ids[i]] = servers[i].URL
		list = append(list, servers[i].URL)
	}
	for i := range servers {
		NewServer(c.store(i), urls).Register(handlers[i])
	}
	cl := client.New(list)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for i := 0; i < 20; i++ {
		if _, err := cl.Put(ctx, "k"+strconv.Itoa(i), "v"+strconv.Itoa(i)); err != nil {
			t.Fatal(err)
		}
	}
	r, err := cl.Get(ctx, "k7")
	if err != nil || r.Value != "v7" {
		t.Fatalf("get k7 = %+v %v", r, err)
	}
	r, err = cl.CAS(ctx, "k7", "v7", "new")
	if err != nil || !r.Applied {
		t.Fatalf("cas = %+v %v", r, err)
	}
	// Kill the leader; the client must find the new one.
	for i := range c.nodes {
		if _, isLeader := c.nodes[i].State(); isLeader {
			c.crash(i)
			servers[i].Close() // a crashed process stops serving HTTP too
			break
		}
	}
	if _, err := cl.Put(ctx, "after", "failover"); err != nil {
		t.Fatalf("put after leader crash: %v", err)
	}
	if r, err := cl.Get(ctx, "k7"); err != nil || r.Value != "new" {
		t.Fatalf("after failover get = %+v %v", r, err)
	}
}
