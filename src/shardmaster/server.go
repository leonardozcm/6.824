package shardmaster

import (
	"log"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0
const SHARDMASTER_LOG_HEAD = "Shard Master head --- "

func DPrintf(format string, a ...interface{}) (n int, err error) {
	log.SetFlags(log.Lmicroseconds)
	pc, _, _, _ := runtime.Caller(1)
	func_name := runtime.FuncForPC(pc).Name()
	if Debug > 0 {
		log.Printf(SHARDMASTER_LOG_HEAD+func_name+" -- "+format, a...)
	}
	return
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs       []Config // indexed by config num
	waitChan      map[string]chan CommonReply
	lastProcessId map[string]int64
	stopCh        chan struct{}
}

type OpNames string

const (
	Join  OpNames = "Join"
	Leave OpNames = "Leave"
	Move  OpNames = "Move"
	Query OpNames = "Query"
)

type Op struct {
	// Your data here.
	OpName    OpNames
	CommandId string
	OpArgs    interface{}
}

const Nshards = 10
const changable = -1

func arrangeShards(oldShards [Nshards]int, newGids []int) (newShards [Nshards]int) {
	// count old gids
	DPrintf("oldShards ", oldShards)
	oldGidsMap := make(map[int]int)
	for _, gid := range oldShards {
		if _, ok := oldGidsMap[gid]; !ok {
			oldGidsMap[gid] = 0
		}
	}

	// All leaves
	if len(newGids) == 0 {
		for i := 0; i < Nshards; i++ {
			newShards[i] = 0
		}
		return
	}

	newShards = [10]int{}
	for i := 0; i < Nshards; i++ {
		newShards[i] = oldShards[i]
	}

	// Union two set
	union := map[int]bool{}
	newJoined := map[int]bool{}
	newGidsCounter := map[int]int{}
	for _, gid := range newGids {
		if isInMap(oldGidsMap, gid) {
			union[gid] = true
		} else {
			newJoined[gid] = true
		}
		newGidsCounter[gid] = 0
	}

	// All gids that keeps shards remains the same
	if len(union) == Nshards {
		return
	}

	// Edge case:
	// Join, oldgids>=Nshards, newGids>Nshards,union must==Nshards no change
	// Join, oldgids<Nshards, newGids>Nshards, union must<Nshards, randomly append gids to Nshards
	// Leave, oldgids>Nshards, newGids<Nshards, union must<Nshards, follow regular route
	// Leave, oldgids>Nshards, newGids>=Nshards, union==Nshards, follow regular route
	// Leave, oldgids>Nshards, newGids>=Nshards, union<Nshards, randomly append gids to Nshards

	// oldgids < Nshards and newgids > Nshards
	// fetch Nshards from newgids including unions
	if len(newGids) > Nshards {
		newJoinedTmp := map[int]bool{}
		counter := Nshards - len(union)
		for k, v := range newJoined {
			newJoinedTmp[k] = v
			if counter--; counter == 0 {
				break
			}
		}
		newJoined = newJoinedTmp
	}
	DPrintf("newJoined", newJoined)

	// new shards count
	avgShards := Nshards / (len(union) + len(newJoined))
	maxShards := avgShards + 1
	maxShardsNum := Nshards % (len(union) + len(newJoined))

	// mark all available slots as changable
	// DPrintf("Union {%v}", union)
	DPrintf("avgShards %d, maxShards %d, maxShardsNum %d\n", avgShards, maxShards, maxShardsNum)
	for i := 0; i < Nshards; i++ {
		if _, ok := union[newShards[i]]; !ok {
			// mark as changable
			newShards[i] = changable
		} else {
			oldGidsMap[newShards[i]]++
			// In Join case, a existing shards trigger the limitation of maxshards
			// and there could still remain some maxshardsNum to let this shards stay sticked
			// skip this one as needed
			if oldGidsMap[newShards[i]] == maxShards &&
				maxShardsNum > 0 {
				// mark as changable
				maxShardsNum--
				continue
			} else if oldGidsMap[newShards[i]] > avgShards {
				newShards[i] = changable
			}
		}
	}
	DPrintf("mask result ", newShards)

	// Append each elements to >=avgShardsNum

	// Is empty if no new gid joins
	// arrange newJoined first
	// So this is for Join only
	ptr := findNextChangable(newShards, 0)
	for k := range newJoined {
		appendNum := avgShards
		if maxShardsNum > 0 {
			appendNum = maxShards
			maxShardsNum--
		}
		DPrintf("appendNum is ", appendNum)
		for i := 0; i < appendNum; i++ {
			newShards[ptr] = k
			ptr = findNextChangable(newShards, ptr)
		}
	}
	DPrintf("after Join newshards is", newShards)
	if len(newJoined) != 0 {
		if ptr != -1 {
			log.Fatalln("Assumpation error, ptr should be at the end for the array.")
		}
		return
	}

	// For Leave only
	for i := 0; i < Nshards; i++ {
		if newShards[i] != changable {
			newGidsCounter[newShards[i]]++
		}
	}

	DPrintf("newGidsCounter: ", newGidsCounter)
	ptr = findNextChangable(newShards, 0)
	for k := range union {
		for ; newGidsCounter[k] < avgShards; ptr = findNextChangable(newShards, ptr) {
			newShards[ptr] = k
			// ptr = findNextChangable(newShards, ptr)
			newGidsCounter[k]++
		}
	}
	DPrintf("avg res is ", newShards)
	for k := range union {
		if ptr == -1 {
			break
		}
		newShards[ptr] = k
		ptr = findNextChangable(newShards, ptr)
	}
	if ptr != -1 {
		log.Fatalln("Assumpation error, ptr should be at the end for the array.")
	}
	return
}

func findNextChangable(newShards [Nshards]int, start int) int {
	for ; start < Nshards; start++ {
		if newShards[start] == changable {
			return start
		}
	}
	return -1
}

func isInMap(m map[int]int, key int) bool {
	_, ok := m[key]
	return ok
}

func (sm *ShardMaster) delOpChan(commandId string) {
	DPrintf("Server %d ready to delete opChan %v", sm.me, commandId)
	sm.mu.Lock()
	defer sm.mu.Unlock()

	delete(sm.waitChan, commandId)
	DPrintf("Server %d delete opChan %v", sm.me, commandId)
}

func (sm *ShardMaster) startOps(opName OpNames, commandId string, opArgs interface{}) (res CommonReply) {
	DPrintf("Server %d startCMD", sm.me)
	op := Op{OpName: opName,
		CommandId: commandId,
		OpArgs:    opArgs,
	}
	DPrintf("Server %d start OP %s", sm.me, op.OpName)
	_, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		res.WrongLeader = true
		DPrintf("Server %d Wrong Leader", sm.me)
	} else {
		DPrintf("Send to leader %d, ", sm.me)
		sm.mu.Lock()
		opChan := make(chan CommonReply, 3)
		sm.waitChan[op.CommandId] = opChan
		sm.mu.Unlock()
		select {
		case res = <-opChan:
			DPrintf("Server %d get opChan %v", sm.me, commandId)
			sm.delOpChan(commandId)
			DPrintf("Leader %d Success in %v, reply is %v", sm.me, op, res)
		case <-time.After(SMServer_TimeOut):
			DPrintf("Leader %d %v time out commandId %s", sm.me, op.OpName, commandId)
			sm.delOpChan(commandId)
		}
	}
	return
}

func (sm *ShardMaster) extractClientID(value string) (string, int64) {
	DPrintf("Server %d extractClientID %v", sm.me, value)
	i, _ := strconv.ParseInt(strings.Fields(value)[1], 10, 64)
	return strings.Fields(value)[0], i
}

func (sm *ShardMaster) checkDuplicatedRequest(clientId string, commandId int64) bool {
	if val, ok := sm.lastProcessId[clientId]; ok {
		DPrintf("sm.lastProcessId[clientId]: %v, commandId %v", sm.lastProcessId[clientId], commandId)
		return val == commandId
	}
	return false
}

func (sm *ShardMaster) onJoin(args *JoinArgs) (reply CommonReply) {

	latestConfig := sm.getLatestConfig()

	new_groups := make(map[int][]string)

	for k, v := range latestConfig.Groups {
		new_groups[k] = v
	}
	for k, v := range args.Servers {
		new_groups[k] = v
	}

	servers := []int{}
	for gid := range new_groups {
		servers = append(servers, gid)
	}
	new_shards := [NShards]int{}
	if len(latestConfig.Groups) == 0 {
		// First Join
		for i := 0; i < NShards; i++ {
			new_shards[i] = servers[i%len(servers)]
		}
	} else {
		new_shards = arrangeShards(latestConfig.Shards, servers)
	}

	new_config := Config{
		Num:    len(sm.configs),
		Shards: new_shards,
		Groups: new_groups,
	}

	sm.configs = append(sm.configs, new_config)

	reply.Err = OK
	reply.WrongLeader = false
	return
}

func (sm *ShardMaster) onLeave(args *LeaveArgs) (reply CommonReply) {
	latestConfig := sm.getLatestConfig()

	new_groups := make(map[int][]string)

	for k, v := range latestConfig.Groups {
		new_groups[k] = v
	}

	leave_gids := args.GIDs
	for _, gid := range leave_gids {
		delete(new_groups, gid)
	}
	DPrintf("Leaved new_groups %v", new_groups)
	servers := []int{}
	for gid := range new_groups {
		servers = append(servers, gid)
	}
	// Assume there is at least one GID
	new_shards := arrangeShards(latestConfig.Shards, servers)

	new_config := Config{
		Num:    len(sm.configs),
		Shards: new_shards,
		Groups: new_groups,
	}

	sm.configs = append(sm.configs, new_config)
	reply.Err = OK
	reply.WrongLeader = false

	return
}

func (sm *ShardMaster) onMove(args *MoveArgs) (reply CommonReply) {
	latestConfig := sm.getLatestConfig()

	new_groups := make(map[int][]string)

	for k, v := range latestConfig.Groups {
		new_groups[k] = v
	}

	move_gids := args.GID
	move_shards := args.Shard
	new_shards := [Nshards]int{}
	for i := 0; i < NShards; i++ {
		if i == move_shards {
			new_shards[i] = move_gids
		} else {
			new_shards[i] = latestConfig.Shards[i]
		}
	}

	new_config := Config{
		Num:    len(sm.configs),
		Shards: new_shards,
		Groups: new_groups,
	}

	sm.configs = append(sm.configs, new_config)
	reply.Err = OK
	reply.WrongLeader = false

	return
}

func (sm *ShardMaster) onQuery(args *QueryArgs) (reply CommonReply) {
	queryNum := args.Num
	var latestConfig Config
	if queryNum > len(sm.configs)-1 || queryNum < 0 {
		latestConfig = sm.getLatestConfig()
	} else {
		latestConfig = sm.configs[queryNum]
	}

	reply.Err = OK
	reply.WrongLeader = false
	reply.Config = latestConfig
	return
}

func (sm *ShardMaster) getLatestConfig() Config {
	return sm.configs[len(sm.configs)-1]
}

func (sm *ShardMaster) processOps(applyMsg raft.ApplyMsg) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	DPrintf("Server %d get for applyCh %v", sm.me, applyMsg)

	// Check Snapshot
	if !applyMsg.CommandValid {
		return
	}

	clientId, commandId := sm.extractClientID(applyMsg.Command.(Op).CommandId)
	// replyOp := Op{Key: applyMsg.Command.(Op).Key,
	// 	Op_name:   applyMsg.Command.(Op).Op_name,
	// 	CommandId: applyMsg.Command.(Op).CommandId,
	// 	Value:     applyMsg.Command.(Op).Value}
	reply := CommonReply{
		WrongLeader: true,
		Err:         OK,
		Config:      Config{},
	}
	if sm.checkDuplicatedRequest(clientId, commandId) {
		DPrintf("Server %d do nothing when process a duplicated apply %v",
			sm.me, applyMsg)
	} else {
		op := applyMsg.Command.(Op)
		switch op.OpName {
		case Join:
			// sm.kvMap[applyMsg.Command.(Op).Key] = applyMsg.Command.(Op).Value
			arg := op.OpArgs.(JoinArgs)
			reply = sm.onJoin(&arg)
			sm.lastProcessId[clientId] = commandId
		case Leave:
			// sm.kvMap[applyMsg.Command.(Op).Key] += applyMsg.Command.(Op).Value
			arg := op.OpArgs.(LeaveArgs)
			reply = sm.onLeave(&arg)
			sm.lastProcessId[clientId] = commandId
		case Move:
			arg := op.OpArgs.(MoveArgs)
			reply = sm.onMove(&arg)
			sm.lastProcessId[clientId] = commandId
		case Query:
			arg := op.OpArgs.(QueryArgs)
			reply = sm.onQuery(&arg)
		}
	}

	// detect if a snapshot should be executed
	// kv.TrySaveSnapshot(applyMsg.CommandIndex)

	DPrintf("Server %d finshed process op %v", sm.me, reply)
	if opChan, ok := sm.waitChan[applyMsg.Command.(Op).CommandId]; ok {
		DPrintf("Server %d append %v.", sm.me, reply)
		if len(opChan) > 0 {
			return
		}
		opChan <- reply
		DPrintf("Server %d finish appending %v.", sm.me, reply)
	}

}

func (sm *ShardMaster) ProcessOpsInTime() {
	var applyMsg raft.ApplyMsg
	for {
		select {
		case <-sm.stopCh:
			DPrintf("Server %d exits", sm.me)
			return
		case applyMsg = <-sm.applyCh:
			DPrintf("Server %d process op %v in the background", sm.me, applyMsg.Command)
			sm.processOps(applyMsg)
			DPrintf("Server %d process op %v in the background done", sm.me, applyMsg.Command)
		}
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	res := sm.startOps(Join, args.CommandId, *args)
	reply.Err = res.Err
	reply.WrongLeader = res.WrongLeader
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	res := sm.startOps(Leave, args.CommandId, *args)
	reply.Err = res.Err
	reply.WrongLeader = res.WrongLeader
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	res := sm.startOps(Move, args.CommandId, *args)
	reply.Err = res.Err
	reply.WrongLeader = res.WrongLeader
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	res := sm.startOps(Query, args.CommandId, *args)
	reply.Err = res.Err
	reply.WrongLeader = res.WrongLeader
	reply.Config = res.Config
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	DPrintf("Server %d Kill", sm.me)
	close(sm.stopCh)
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	labgob.Register(Op{})
	labgob.Register(map[string]string{})
	labgob.Register(map[string]int64{})
	labgob.Register(JoinArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(QueryArgs{})
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.waitChan = make(map[string]chan CommonReply)
	sm.lastProcessId = make(map[string]int64)
	sm.stopCh = make(chan struct{})

	go sm.ProcessOpsInTime()
	return sm
}
