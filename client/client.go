// Package client is a Go client for the KV cluster. It finds the leader,
// follows 421 redirects, and retries writes with the same (ClientID, Seq) so
// each write takes effect exactly once.
package client

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

// Result mirrors kv.Result.
type Result struct {
	Value   string `json:"value"`
	Found   bool   `json:"found"`
	Applied bool   `json:"applied"`
}

// Client is safe for concurrent use; writes from one Client are serialised
// so sequence numbers stay ordered.
type Client struct {
	nodes []string
	http  *http.Client
	id    string

	mu     sync.Mutex // serialises writes
	seq    uint64
	leader string
	lmu    sync.Mutex
}

// New returns a client for the given node API URLs.
func New(nodes []string) *Client {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return &Client{
		nodes: nodes,
		http:  &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 64}},
		id:    hex.EncodeToString(b),
	}
}

// ErrGiveUp is returned when no leader answered before the context ended.
var ErrGiveUp = errors.New("client: no leader reachable")

func (c *Client) Get(ctx context.Context, key string) (Result, error) {
	return c.do(ctx, http.MethodGet, "/kv/"+url.PathEscape(key), nil, 0)
}

func (c *Client) Put(ctx context.Context, key, value string) (Result, error) {
	return c.write(ctx, http.MethodPut, "/kv/"+url.PathEscape(key), []byte(value))
}

func (c *Client) Delete(ctx context.Context, key string) (Result, error) {
	return c.write(ctx, http.MethodDelete, "/kv/"+url.PathEscape(key), nil)
}

// CAS sets key to value only if its current value equals expect ("" = absent).
func (c *Client) CAS(ctx context.Context, key, expect, value string) (Result, error) {
	body, _ := json.Marshal(map[string]string{"expect": expect, "value": value})
	return c.write(ctx, http.MethodPost, "/kv/"+url.PathEscape(key)+"/cas", body)
}

func (c *Client) write(ctx context.Context, method, path string, body []byte) (Result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	return c.do(ctx, method, path, body, c.seq)
}

func (c *Client) do(ctx context.Context, method, path string, body []byte, seq uint64) (Result, error) {
	backoff := 20 * time.Millisecond
	try := 0
	for {
		target := c.pick(try)
		try++
		res, leader, err := c.once(ctx, method, target+path, body, seq)
		if err == nil {
			c.setLeader(target)
			return res, nil
		}
		// Follow a redirect unless it points back at the node we just asked
		// (a deposed or isolated leader that hasn't noticed yet).
		if leader != "" && leader != target {
			c.setLeader(leader)
		} else {
			c.setLeader("")
		}
		select {
		case <-ctx.Done():
			return Result{}, fmt.Errorf("%w: %v", ErrGiveUp, err)
		case <-time.After(backoff):
		}
		if backoff < 300*time.Millisecond {
			backoff *= 2
		}
	}
}

func (c *Client) pick(try int) string {
	c.lmu.Lock()
	defer c.lmu.Unlock()
	if c.leader != "" {
		return c.leader
	}
	return c.nodes[try%len(c.nodes)]
}

func (c *Client) setLeader(l string) {
	c.lmu.Lock()
	c.leader = l
	c.lmu.Unlock()
}

func (c *Client) once(ctx context.Context, method, u string, body []byte, seq uint64) (Result, string, error) {
	req, err := http.NewRequestWithContext(ctx, method, u, bytes.NewReader(body))
	if err != nil {
		return Result{}, "", err
	}
	if seq > 0 {
		req.Header.Set("X-Client-ID", c.id)
		req.Header.Set("X-Seq", strconv.FormatUint(seq, 10))
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return Result{}, "", err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	switch resp.StatusCode {
	case http.StatusOK:
		var r Result
		err := json.Unmarshal(data, &r)
		return r, "", err
	case http.StatusMisdirectedRequest:
		var e struct{ Leader string }
		_ = json.Unmarshal(data, &e)
		return Result{}, e.Leader, errors.New("not leader")
	default:
		return Result{}, "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, bytes.TrimSpace(data))
	}
}
