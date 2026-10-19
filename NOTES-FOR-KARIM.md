# Notes for Karim: Raft KV Store

## Resume bullets
- Implemented **Raft consensus from scratch in Go** (elections, log replication, snapshots, ReadIndex). It powers a replicated KV store verified **linearizable by porcupine over 10,675 operations** under network partitions and node crashes.
- Built fault-injection tests (packet loss, reordering, partitions, crash/restart). They caught a real split-vote livelock caused by clock-seeded RNGs on Windows, now fixed with a regression test.
- Ran a 3-node Docker cluster at **688 fsynced writes/s and 3,244 linearizable reads/s**; on leader failure, writes recovered within **1.38 s with zero failed client writes**.

## Likely interview questions
1. **Why does a leader commit only entries from its own term?** Figure 8. An old-term entry on a majority can still be overwritten by a future leader. Committing a current-term entry commits everything before it, which is why there's a no-op on election.
2. **How are reads linearizable without writing to the log?** ReadIndex: note commitIndex, confirm leadership via a majority of heartbeat acks, wait until applied ≥ that index.
3. **What must be persisted, and when?** term, votedFor, and the log, before answering any RPC. Otherwise a node could vote twice in one term.
4. **How do retries stay exactly-once?** (clientID, seq) per session. The state machine stores the last seq and its result per client, and the session table is part of the snapshot.
5. **What happens when a follower falls far behind?** If its nextIndex is ≤ the leader's snapshot index, the leader sends InstallSnapshot instead of log entries.

## Understand before interviews
- `raft.go` → `HandleAppendEntries`: the prev-index check, conflict-hint computation, truncating on conflict, and why commitIndex = min(leaderCommit, last new entry).
- `advanceCommit`: sort the match indexes, take the quorum-th value, require the current term.
- The flusher (group commit): the leader's own match index is its fsynced index.
- The crypto seed bug: why identical timeouts cause endless split votes.
