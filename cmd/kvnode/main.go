// Command kvnode runs one member of the replicated KV cluster.
//
//	kvnode -id n1 -listen :8001 -data ./data/n1 \
//	  -cluster n1=http://localhost:8001,n2=http://localhost:8002,n3=http://localhost:8003
//
// Every flag can also be set by environment variable (ID, LISTEN, DATA_DIR,
// CLUSTER, SNAPSHOT_BYTES).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Alkarim786/raft-kv-store/kv"
	"github.com/Alkarim786/raft-kv-store/raft"
	"github.com/Alkarim786/raft-kv-store/transport"
)

func main() {
	id := flag.String("id", env("ID", ""), "this node's ID (must appear in -cluster)")
	listen := flag.String("listen", env("LISTEN", ":8001"), "HTTP listen address")
	dataDir := flag.String("data", env("DATA_DIR", "data"), "directory for Raft state and snapshots")
	cluster := flag.String("cluster", env("CLUSTER", ""), "comma-separated id=url for every node, including this one")
	snapBytes := flag.Int("snapshot-bytes", envInt("SNAPSHOT_BYTES", 4<<20), "snapshot when Raft state exceeds this many bytes (0 = never)")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(log, *id, *listen, *dataDir, *cluster, *snapBytes); err != nil {
		log.Error("exit", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, id, listen, dataDir, cluster string, snapBytes int) error {
	members, err := parseCluster(cluster)
	if err != nil {
		return err
	}
	if _, ok := members[id]; !ok {
		return fmt.Errorf("-id %q is not in -cluster", id)
	}
	var peers []string
	for m := range members {
		if m != id {
			peers = append(peers, m)
		}
	}
	storage, err := raft.NewFileStorage(dataDir)
	if err != nil {
		return err
	}
	applyCh := make(chan raft.ApplyMsg, 256)
	node, err := raft.New(raft.Config{
		ID: id, Peers: peers, Storage: storage, Transport: transport.NewHTTP(members),
		ApplyCh: applyCh, Logger: log,
	})
	if err != nil {
		return err
	}
	store := kv.NewStore(node, storage, applyCh, snapBytes)

	mux := http.NewServeMux()
	transport.Register(mux, node)
	kv.NewServer(store, members).Register(mux)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	srv := &http.Server{Addr: listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	log.Info("kvnode started", "id", id, "listen", listen, "peers", peers, "data", dataDir)

	select {
	case <-ctx.Done():
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdown)
	node.Stop()
	store.Stop()
	return nil
}

func parseCluster(s string) (map[string]string, error) {
	out := map[string]string{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, u, ok := strings.Cut(part, "=")
		if !ok || id == "" || u == "" {
			return nil, fmt.Errorf("bad -cluster entry %q (want id=url)", part)
		}
		out[id] = strings.TrimRight(u, "/")
	}
	if len(out) == 0 {
		return nil, errors.New("-cluster is required")
	}
	return out, nil
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		var n int
		if _, err := fmt.Sscan(v, &n); err == nil {
			return n
		}
	}
	return def
}
