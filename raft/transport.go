package raft

import "context"

// Transport sends RPCs to peers. Implementations: MemNetwork (tests, with
// fault injection) and transport.HTTP (real deployments).
type Transport interface {
	RequestVote(ctx context.Context, peer string, args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(ctx context.Context, peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	InstallSnapshot(ctx context.Context, peer string, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}

// Handler is the receiving side of the RPCs; *Node implements it.
type Handler interface {
	HandleRequestVote(args *RequestVoteArgs) (*RequestVoteReply, error)
	HandleAppendEntries(args *AppendEntriesArgs) (*AppendEntriesReply, error)
	HandleInstallSnapshot(args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}
