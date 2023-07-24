package kvraft

import (
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 1
const KVRAFT_LOG_HEAD = "Kvraft head --- "

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(KVRAFT_LOG_HEAD+format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op_name   string
	CommandId string
	Key       string
	Value     string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvMap         map[string]string
	lastProcessId map[string][2]interface{}
}

func (kv *KVServer) extractClientID(value string) (string, int64) {
	i, _ := strconv.ParseInt(strings.Fields(value)[1], 10, 64)
	return strings.Fields(value)[0], i
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	clientId, commandId := kv.extractClientID(args.CommandId)
	if kv.lastProcessId[clientId][0] != nil &&
		kv.lastProcessId[clientId][0].(int64) >= commandId {
		DPrintf("Get kv.lastProcessId[clientId]: %d, commandId %v", kv.lastProcessId[clientId], commandId)
		reply_stored := kv.lastProcessId[clientId][1].(GetReply)
		reply.Err = reply_stored.Err
		reply.Value = reply_stored.Value
		return
	}

	op := Op{"Get", args.CommandId, args.Key, ""}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
	} else {
		<-kv.applyCh
		if value, ok := kv.kvMap[args.Key]; ok {
			reply.Value = value
			reply.Err = OK
		} else {
			reply.Err = ErrNoKey
		}
		kv.lastProcessId[clientId] = [2]interface{}{commandId, GetReply{reply.Err, reply.Value}}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// DPrintf("PutAppend: %v", args)
	clientId, commandId := kv.extractClientID(args.CommandId)
	if kv.lastProcessId[clientId][0] != nil &&
		kv.lastProcessId[clientId][0].(int64) >= commandId {
		DPrintf("PutAppend kv.lastProcessId[clientId]: %d, commandId %v", kv.lastProcessId[clientId], commandId)
		reply.Err = OK
		return
	}

	op := Op{args.Op, args.CommandId, args.Key, args.Value}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
	} else {
		DPrintf("Send to leader %d, ", kv.me)
		// success apply get to state machine
		applyMsg := <-kv.applyCh
		switch applyMsg.Command.(Op).Op_name {
		case "Put":
			kv.kvMap[applyMsg.Command.(Op).Key] = applyMsg.Command.(Op).Value
		case "Append":
			kv.kvMap[applyMsg.Command.(Op).Key] += applyMsg.Command.(Op).Value
		}
		reply.Err = OK
		kv.lastProcessId[clientId] = [2]interface{}{commandId, PutAppendReply{Err: OK}}
		DPrintf("Leader %d Success in %s", kv.me, applyMsg.Command.(Op).Key)
	}
}

func (kv *KVServer) ProcessOpsInTime(t *time.Timer) {
	// defer kv.mu.Unlock()

	var applyMsg raft.ApplyMsg
	for {
		select {
		case applyMsg = <-kv.applyCh:
			kv.mu.Lock()
			DPrintf("Server %d get for applyCh %v", kv.me, applyMsg)

			clientId, commandId := kv.extractClientID(applyMsg.Command.(Op).CommandId)
			switch applyMsg.Command.(Op).Op_name {
			case "Put":
				kv.kvMap[applyMsg.Command.(Op).Key] = applyMsg.Command.(Op).Value
				kv.lastProcessId[clientId] = [2]interface{}{commandId, PutAppendReply{Err: OK}}
			case "Append":
				kv.kvMap[applyMsg.Command.(Op).Key] += applyMsg.Command.(Op).Value
				kv.lastProcessId[clientId] = [2]interface{}{commandId, PutAppendReply{Err: OK}}
			case "Get":
				reply := GetReply{}
				if value, ok := kv.kvMap[applyMsg.Command.(Op).Key]; ok {
					reply.Value = value
					reply.Err = OK
				} else {
					reply.Err = ErrNoKey
				}
				kv.lastProcessId[clientId] = [2]interface{}{commandId, reply}
			}
			DPrintf("Server %d process op %v", kv.me, applyMsg.Command.(Op))
			kv.mu.Unlock()
		case <-t.C:
			t.Reset(100 * time.Millisecond)
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.kvMap = map[string]string{}
	kv.lastProcessId = make(map[string][2]interface{})

	// You may need initialization code here.
	process_timer := time.NewTimer(100 * time.Millisecond)
	go kv.ProcessOpsInTime(process_timer)

	return kv
}
