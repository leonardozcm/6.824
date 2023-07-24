package kvraft

import "time"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

const Clerk_Req_During = 50 * time.Millisecond
const KvServer_TimeOut = 10 * time.Millisecond

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CommandId string
}

type PutAppendReply struct {
	Err Err
	// CommandId int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	CommandId string
}

type GetReply struct {
	Err   Err
	Value string

	// CommandId int
}
