// Command kvbench measures a running cluster.
//
//	kvbench -nodes http://localhost:8001,http://localhost:8002,http://localhost:8003 -mode write -c 32 -d 10s
//	kvbench ... -mode read
//	kvbench ... -mode failover -d 30s   # run while you kill the leader; reports the longest write outage
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Alkarim786/raft-kv-store/client"
)

type report struct {
	Mode        string  `json:"mode"`
	Concurrency int     `json:"concurrency"`
	DurationS   float64 `json:"duration_s"`
	Ops         int64   `json:"ops"`
	Errors      int64   `json:"errors"`
	OpsPerSec   float64 `json:"ops_per_sec"`
	P50ms       float64 `json:"p50_ms"`
	P90ms       float64 `json:"p90_ms"`
	P99ms       float64 `json:"p99_ms"`
	MaxGapMs    float64 `json:"max_gap_ms,omitempty"`
}

func main() {
	nodes := flag.String("nodes", "http://localhost:8001,http://localhost:8002,http://localhost:8003", "node API URLs")
	mode := flag.String("mode", "write", "write | read | failover")
	conc := flag.Int("c", 16, "concurrent clients")
	dur := flag.Duration("d", 10*time.Second, "duration")
	keys := flag.Int("keys", 1000, "key space")
	flag.Parse()
	urls := strings.Split(*nodes, ",")

	if *mode == "read" {
		seed := client.New(urls)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		for k := 0; k < *keys; k++ {
			if _, err := seed.Put(ctx, "k"+strconv.Itoa(k), "v"); err != nil {
				fmt.Fprintln(os.Stderr, "seed:", err)
				os.Exit(1)
			}
		}
		cancel()
	}

	var ops, errs atomic.Int64
	lat := make([][]time.Duration, *conc)
	var lastOK atomic.Int64
	lastOK.Store(time.Now().UnixNano())
	var maxGap atomic.Int64
	stop := time.Now().Add(*dur)
	var wg sync.WaitGroup
	for w := 0; w < *conc; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			c := client.New(urls) // one client (session) per worker
			for i := 0; time.Now().Before(stop); i++ {
				key := "k" + strconv.Itoa((w*7919+i)%*keys)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				t0 := time.Now()
				var err error
				if *mode == "read" {
					_, err = c.Get(ctx, key)
				} else {
					_, err = c.Put(ctx, key, strconv.Itoa(i))
				}
				cancel()
				el := time.Since(t0)
				if err != nil {
					errs.Add(1)
					continue
				}
				ops.Add(1)
				lat[w] = append(lat[w], el)
				now := time.Now().UnixNano()
				prev := lastOK.Swap(now)
				if gap := now - prev; gap > maxGap.Load() {
					maxGap.Store(gap)
				}
			}
		}(w)
	}
	start := time.Now()
	wg.Wait()
	elapsed := time.Since(start).Seconds()
	var all []time.Duration
	for _, l := range lat {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	p := func(q float64) float64 {
		if len(all) == 0 {
			return 0
		}
		return float64(all[int(q*float64(len(all)-1))].Microseconds()) / 1000
	}
	r := report{
		Mode: *mode, Concurrency: *conc, DurationS: elapsed, Ops: ops.Load(), Errors: errs.Load(),
		OpsPerSec: float64(ops.Load()) / elapsed, P50ms: p(.5), P90ms: p(.9), P99ms: p(.99),
	}
	if *mode == "failover" {
		r.MaxGapMs = float64(maxGap.Load()) / 1e6
	}
	_ = json.NewEncoder(os.Stdout).Encode(r)
}
