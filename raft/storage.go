package raft

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// PersistentState is what Raft must keep on stable storage before answering
// any RPC (Figure 2): the current term, the vote, and the log. Log[0] is a
// sentinel holding the snapshot's last included index and term.
type PersistentState struct {
	Term     uint64
	VotedFor string
	Log      []Entry
}

// Storage persists Raft state and the latest snapshot atomically.
type Storage interface {
	// Save persists state. If snapshot is non-nil it replaces the stored
	// snapshot in the same atomic step.
	Save(st PersistentState, snapshot []byte) error
	// Load returns the saved state (ok=false if none) and snapshot.
	Load() (st PersistentState, snapshot []byte, ok bool, err error)
	// StateSize is the size in bytes of the persisted state (excluding the
	// snapshot); the KV layer uses it to decide when to snapshot.
	StateSize() int
}

func encodeState(st PersistentState) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(st); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeState(b []byte) (PersistentState, error) {
	var st PersistentState
	err := gob.NewDecoder(bytes.NewReader(b)).Decode(&st)
	return st, err
}

// MemoryStorage keeps state in memory. Tests "crash" a node by stopping it
// and starting a new Node on the same MemoryStorage.
type MemoryStorage struct {
	mu       sync.Mutex
	state    []byte
	snapshot []byte
}

// NewMemoryStorage returns empty in-memory storage.
func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{} }

func (m *MemoryStorage) Save(st PersistentState, snapshot []byte) error {
	b, err := encodeState(st)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = b
	if snapshot != nil {
		m.snapshot = append([]byte(nil), snapshot...)
	}
	return nil
}

func (m *MemoryStorage) Load() (PersistentState, []byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state == nil {
		return PersistentState{}, nil, false, nil
	}
	st, err := decodeState(m.state)
	return st, append([]byte(nil), m.snapshot...), err == nil, err
}

func (m *MemoryStorage) StateSize() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.state)
}

// FileStorage writes state and snapshot to a directory. Each write goes to a
// temp file which is fsynced and then renamed over the old file, so a crash
// leaves either the old or the new version, never a torn one.
type FileStorage struct {
	dir  string
	mu   sync.Mutex
	size int
}

// NewFileStorage creates dir if needed.
func NewFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	fs := &FileStorage{dir: dir}
	if fi, err := os.Stat(filepath.Join(dir, "state.bin")); err == nil {
		fs.size = int(fi.Size())
	}
	return fs, nil
}

func (f *FileStorage) Save(st PersistentState, snapshot []byte) error {
	b, err := encodeState(st)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	// Snapshots are named by their last included index, and the state's
	// sentinel entry (Log[0]) names the snapshot it goes with. Order of writes:
	// new snapshot file, then state, then delete old snapshots. A crash at any
	// point leaves a state file whose snapshot file still exists.
	if snapshot != nil {
		if err := atomicWrite(f.snapshotPath(st.Log[0].Index), snapshot); err != nil {
			return err
		}
	}
	if err := atomicWrite(filepath.Join(f.dir, "state.bin"), b); err != nil {
		return err
	}
	f.size = len(b)
	if snapshot != nil {
		keep := filepath.Base(f.snapshotPath(st.Log[0].Index))
		old, _ := filepath.Glob(filepath.Join(f.dir, "snapshot-*.bin"))
		for _, o := range old {
			if filepath.Base(o) != keep {
				_ = os.Remove(o)
			}
		}
	}
	return nil
}

func (f *FileStorage) snapshotPath(index uint64) string {
	return filepath.Join(f.dir, fmt.Sprintf("snapshot-%020d.bin", index))
}

func (f *FileStorage) Load() (PersistentState, []byte, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, err := os.ReadFile(filepath.Join(f.dir, "state.bin"))
	if errors.Is(err, os.ErrNotExist) {
		return PersistentState{}, nil, false, nil
	}
	if err != nil {
		return PersistentState{}, nil, false, err
	}
	st, err := decodeState(b)
	if err != nil {
		return PersistentState{}, nil, false, fmt.Errorf("decode state: %w", err)
	}
	var snap []byte
	if len(st.Log) > 0 && st.Log[0].Index > 0 {
		if snap, err = os.ReadFile(f.snapshotPath(st.Log[0].Index)); err != nil {
			return PersistentState{}, nil, false, fmt.Errorf("snapshot for index %d: %w", st.Log[0].Index, err)
		}
	}
	return st, snap, true, nil
}

func (f *FileStorage) StateSize() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.size
}

func atomicWrite(path string, data []byte) error {
	tmp := path + ".tmp"
	fh, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := fh.Write(data); err != nil {
		fh.Close()
		return err
	}
	if err := fh.Sync(); err != nil {
		fh.Close()
		return err
	}
	if err := fh.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// Clone returns an independent copy. Tests use it when "crashing" a node so
// that RPC handlers still running in the old instance cannot write into the
// storage the restarted instance uses.
func (m *MemoryStorage) Clone() *MemoryStorage {
	m.mu.Lock()
	defer m.mu.Unlock()
	return &MemoryStorage{
		state:    append([]byte(nil), m.state...),
		snapshot: append([]byte(nil), m.snapshot...),
	}
}
