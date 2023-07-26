package kvraft

import (
	"crypto/rand"
	"math/big"
	"strconv"
	"time"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderIndex int
	clientId    string
}

func getUUID() string {
	return strconv.FormatInt(time.Now().UnixNano(), 10)
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderIndex = ck.RandomSelectServer()
	ck.clientId = strconv.FormatInt(nrand(), 10)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) RandomSelectServer() int {
	return int(nrand()) % (len(ck.servers))
}

func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	reply := GetReply{}
	args := GetArgs{key, ck.clientId + " " + getUUID()}

	for {
		if ok := ck.servers[ck.leaderIndex].Call("KVServer.Get", &args, &reply); ok {
			if reply.Err == ErrWrongLeader {
				ck.leaderIndex = ck.RandomSelectServer()
			} else {
				return reply.Value
			}
		} else {
			ck.leaderIndex = ck.RandomSelectServer()
		}
		time.Sleep(Clerk_Req_During)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	reply := PutAppendReply{}
	DPrintf("Present args: %s / %s / %s", op, key, value)
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		CommandId: ck.clientId + " " + getUUID()}

	for {
		// DPrintf("args: %v", args)
		// DPrintf("ck sending %d to %s %s %s\n", ck.leaderId, op, key, value)
		DPrintf("Sending %v to server %d.", args, ck.leaderIndex)
		if ok := ck.servers[ck.leaderIndex].Call("KVServer.PutAppend", &args, &reply); ok {
			// Err: Wrong leader
			DPrintf("Reply: %v, Command %v", reply, args)
			if reply.Err == OK {
				DPrintf("Client %s Success in %s / %s / %s", ck.clientId, op, key, value)
				return
			} else if reply.Err == ErrWrongLeader {
				ck.leaderIndex = ck.RandomSelectServer()
			}
		} else {
			ck.leaderIndex = ck.RandomSelectServer()
		}
		time.Sleep(Clerk_Req_During)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
