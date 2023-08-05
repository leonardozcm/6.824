package kvraft

import (
	"bytes"
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

const Debug = 0
const KVRAFT_LOG_HEAD = "Kvraft head --- "

func DPrintf(format string, a ...interface{}) (n int, err error) {
	log.SetFlags(log.Lmicroseconds)
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
	stopCh  chan struct{}

	maxraftstate int // snapshot if log grows this big
	persister    raft.Persister

	// Your definitions here.
	kvMap         map[string]string
	waitChan      map[string]chan Op
	lastProcessId map[string]int64
}

func (kv *KVServer) extractClientID(value string) (string, int64) {
	i, _ := strconv.ParseInt(strings.Fields(value)[1], 10, 64)
	return strings.Fields(value)[0], i
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("Server %d start Get: %v", kv.me, args)
	op := Op{"Get", args.CommandId, args.Key, ""}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("Server %d Wrong Leader", kv.me)
	} else {

		kv.mu.Lock()
		var opReply Op
		opChan := make(chan Op, 1)
		kv.waitChan[args.CommandId] = opChan
		kv.mu.Unlock()

		select {
		case opReply = <-opChan:
			kv.delOpChan(args.CommandId)
			if opReply.Value == ErrNoKey {
				reply.Value = ""
				reply.Err = ErrNoKey
			} else {
				reply.Value = opReply.Value
				reply.Err = OK
			}
			DPrintf("Leader %d Success in %v", kv.me, opReply)
		case <-time.After(KvServer_TimeOut):
			DPrintf("Leader %d Get time out", kv.me)
			kv.delOpChan(args.CommandId)
			reply.Err = ErrWrongLeader
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	DPrintf("Server %d start PutAppend: %v", kv.me, args)

	op := Op{args.Op, args.CommandId, args.Key, args.Value}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("Server %d Wrong Leader", kv.me)
	} else {
		DPrintf("Send to leader %d, ", kv.me)
		// success apply get to state machine
		kv.mu.Lock()
		opChan := make(chan Op, 1)
		kv.waitChan[args.CommandId] = opChan
		kv.mu.Unlock()
		DPrintf("leader %d start waiting for %v", kv.me, args)
		select {
		case <-opChan:
			kv.delOpChan(args.CommandId)
			// reply Op success
			reply.Err = OK
			DPrintf("Leader %d Success in %v", kv.me, op)
		case <-time.After(KvServer_TimeOut):
			DPrintf("Leader %d PutAppend time out", kv.me)
			kv.delOpChan(args.CommandId)
			reply.Err = ErrWrongLeader
		}
	}
}

func (kv *KVServer) delOpChan(commandId string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	delete(kv.waitChan, commandId)
}

func (kv *KVServer) checkDuplicatedRequest(clientId string, commandId int64) bool {
	if val, ok := kv.lastProcessId[clientId]; ok {
		DPrintf("kv.lastProcessId[clientId]: %v, commandId %v", kv.lastProcessId[clientId], commandId)
		return val == commandId
	}
	return false
}

func (kv *KVServer) processOps(applyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("Server %d get for applyCh %v", kv.me, applyMsg)

	clientId, commandId := kv.extractClientID(applyMsg.Command.(Op).CommandId)
	replyOp := Op{Key: applyMsg.Command.(Op).Key,
		Op_name:   applyMsg.Command.(Op).Op_name,
		CommandId: applyMsg.Command.(Op).CommandId,
		Value:     applyMsg.Command.(Op).Value}
	if kv.checkDuplicatedRequest(clientId, commandId) {
		DPrintf("Server %d do nothing when process a duplicated apply %v",
			kv.me, applyMsg)
	} else {
		switch applyMsg.Command.(Op).Op_name {
		case "Put":
			kv.kvMap[applyMsg.Command.(Op).Key] = applyMsg.Command.(Op).Value
			kv.lastProcessId[clientId] = commandId
		case "Append":
			kv.kvMap[applyMsg.Command.(Op).Key] += applyMsg.Command.(Op).Value
			kv.lastProcessId[clientId] = commandId
		case "Get":
		}
	}

	// detect if a snapshot should be executed
	// kv.TrySaveSnapshot(applyMsg.CommandIndex)

	if value, ok := kv.kvMap[applyMsg.Command.(Op).Key]; ok {
		replyOp.Value = value
	} else {
		replyOp.Value = ErrNoKey
	}

	DPrintf("Server %d finshed process op %v", kv.me, replyOp)
	if opChan, ok := kv.waitChan[applyMsg.Command.(Op).CommandId]; ok {
		DPrintf("Server %d append %v.", kv.me, replyOp)
		opChan <- replyOp
		DPrintf("Server %d finish appending %v.", kv.me, replyOp)
	}

}

func (kv *KVServer) ProcessOpsInTime(t *time.Timer) {
	// defer kv.mu.Unlock()

	var applyMsg raft.ApplyMsg
	for {
		select {
		case <-kv.stopCh:
			DPrintf("Server %d exits", kv.me)
			return
		case applyMsg = <-kv.applyCh:
			if !applyMsg.CommandValid {
				continue
			}
			DPrintf("Server %d process op %v in the background", kv.me, applyMsg.Command.(Op))
			kv.processOps(applyMsg)
			DPrintf("Server %d process op %v in the background done", kv.me, applyMsg.Command.(Op))
		}
	}
}

func (kv *KVServer) TrySaveSnapshot(index int) {
	// In case the raft logs states have reached the maxraftstate threshold
	// , we need to save the snapshot and discard the old logs
	if kv.persister.RaftStateSize() >= kv.maxraftstate && kv.maxraftstate != -1 {
		kv.SaveSnapshot(index)
	}

}

func (kv *KVServer) SaveSnapshot(index int) {
	labgob.Register(Op{})
	labgob.Register(map[string]string{})
	labgob.Register(map[string]int64{})

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvMap)
	e.Encode(kv.lastProcessId)
	kvdata := w.Bytes()
	kv.rf.AbandonPersistData(index)
	rfdata := kv.rf.GetPersistDataSafe()
	kv.persister.SaveStateAndSnapshot(rfdata, kvdata)
}

func (kv *KVServer) ReadSnapshot() {
	labgob.Register(Op{})
	labgob.Register(map[string]string{})
	labgob.Register(map[string]int64{})

	data := kv.persister.ReadSnapshot()

	var kvMap map[string]string
	var lastProcessId map[string]int64
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&kvMap) != nil ||
		d.Decode(&lastProcessId) != nil {
		DPrintf("Error: Fail to load status from exiting persisted states.")
	} else {
		DPrintf("Server %d Status Loaded", kv.me)
		kv.kvMap = kvMap
		kv.lastProcessId = lastProcessId
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
	close(kv.stopCh)
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
	labgob.Register(map[string]string{})
	labgob.Register(map[string]int64{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.stopCh = make(chan struct{})

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.kvMap = map[string]string{}
	kv.waitChan = map[string]chan Op{}
	kv.lastProcessId = make(map[string]int64)

	// Read Persisted Data
	// kv.ReadSnapshot()

	// You may need initialization code here.
	process_timer := time.NewTimer(100 * time.Millisecond)
	go kv.ProcessOpsInTime(process_timer)

	return kv
}
