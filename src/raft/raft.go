package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type StatusType int

const election_wait_l = 500
const election_wait_r = 800

const (
	Leader StatusType = iota
	Candidate
	Follower
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// Raft id
	n int

	// Persistent state for all servers
	CurrentTerm    int
	VoteFor        int
	Logs           map[int]LogEntry
	applyCh        chan ApplyMsg
	election_timer *time.Timer
	apply_timer    *time.Timer
	stopCh         chan bool

	// Volatile state on all servers
	CommitIndex int
	LastApplied int
	Status      StatusType

	// Volatile state on leaders
	NextIndex  []int
	MatchIndex []int

	// Snapshot storage
	FirstUnsavedIndex int
	LastIncludedTerm  int
	MaxLogIndex       int
	notifyCh          chan struct{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	isleader = (rf.Status == Leader)

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	data := rf.getPersistData()
	DPrintf("Server %d persist data len %d", rf.me, len(data))
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) getPersistData() []byte {

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	DPrintf("Server %d persist data CurrentTerm %d, VoteFor %d, FirstUnsavedIndex %d, MaxLogIndex %d, CommitIndex %d, logs %v", rf.me, rf.CurrentTerm, rf.VoteFor, rf.FirstUnsavedIndex, rf.MaxLogIndex, rf.CommitIndex, rf.Logs)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VoteFor)
	e.Encode(rf.Logs)
	e.Encode(rf.FirstUnsavedIndex)
	e.Encode(rf.LastIncludedTerm)
	e.Encode(rf.MaxLogIndex)
	e.Encode(rf.CommitIndex)
	data := w.Bytes()
	return data
}

func (rf *Raft) GetPersistDataSafe() []byte {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.getPersistData()
}

func (rf *Raft) AbandonPersistData(index int, snapshotData []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.FirstUnsavedIndex >= index {
		DPrintf("Server %d AbandonPersistData failed, rf.FirstUnsavedIndex %d, index %d", rf.me, rf.FirstUnsavedIndex, index)
		return
	}

	rf.LastIncludedTerm = rf.Logs[index].Term
	len_log := len(rf.Logs)
	for i := rf.FirstUnsavedIndex; i <= index; i++ {
		delete(rf.Logs, i)
	}
	DPrintf("Server %d abandon index %d done, before: %d, after: %d", rf.me, index, len_log, len(rf.Logs))
	rf.FirstUnsavedIndex = index + 1
	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), snapshotData)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

func (rf *Raft) InstallSnapshot(arg *InstallSnapshotArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	// defer rf.mu.Unlock()

	reply.Id = rf.me
	reply.Term = rf.CurrentTerm

	if arg.Term < rf.CurrentTerm {
		reply.Success = false
		reply.LastIndex = rf.MaxLogIndex
		reply.FailedTerm = 0
		reply.FirstIndexOfFailedTerm = 0
		DPrintf("Server %d failed show InstallSnapshot args %+v to leader %d", rf.me, reply, arg.LeaderId)
		rf.mu.Unlock()
		return
	}

	if arg.Term > rf.CurrentTerm {
		rf.CurrentTerm = arg.Term
		// in case it's a candidate or old leader
		rf.Status = Follower
		rf.persist()
	}

	if rf.FirstUnsavedIndex > arg.LastIncludedIndex {
		reply.Success = false
		reply.LastIndex = rf.MaxLogIndex
		reply.FailedTerm = 0
		reply.FirstIndexOfFailedTerm = 0
		rf.mu.Unlock()
		return
	}

	abortEnd := rf.MaxLogIndex
	DPrintf("Server %d show InstallSnapshot check rf.MaxLogIndex %d, arg.LastIncludedIndex %d, logs is %v", rf.me, rf.MaxLogIndex, arg.LastIncludedIndex, rf.Logs)
	if _, ok := rf.Logs[arg.LastIncludedIndex]; ok {
		abortEnd = arg.LastIncludedIndex
	}
	DPrintf("Server %d show InstallSnapshot abortEnd %d", rf.me, abortEnd)
	rf.MaxLogIndex = Max(rf.MaxLogIndex, arg.LastIncludedIndex)
	rf.CommitIndex = Max(rf.CommitIndex, arg.LastIncludedIndex)
	rf.LastIncludedTerm = arg.LastIncludedTerm

	for i := rf.FirstUnsavedIndex; i <= abortEnd; i++ {
		delete(rf.Logs, i)
	}
	rf.FirstUnsavedIndex = arg.LastIncludedIndex + 1

	rf.Status = Follower
	rfdata := rf.getPersistData()
	rf.persister.SaveStateAndSnapshot(rfdata, arg.Data)
	// rf.LastApplied = arg.LastIncludedIndex
	rf.election_timer.Reset(RandDuringGenerating(election_wait_l, election_wait_r) * time.Millisecond)

	reply.Success = true
	reply.LastIndex = rf.MaxLogIndex
	reply.FailedTerm = 0
	reply.FirstIndexOfFailedTerm = 0
	DPrintf("Server %d show InstallSnapshot args %+v to leader %d", rf.me, reply, arg.LeaderId)

	rf.mu.Unlock()
	DPrintf("Server %d len(rf.notifyCh) is %d", rf.me, len(rf.notifyCh))
	if len(rf.notifyCh) == 0 {
		rf.notifyCh <- struct{}{}
	}
	// rf.applyCh <- ApplyMsg{false, nil, -1}
}

func (rf *Raft) sendSnapshot(server int, arg *InstallSnapshotArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", arg, reply)
	return ok
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	var currentTerm int
	var voteFor int
	var logs map[int]LogEntry
	var FirstUnsavedIndex int
	var LastIncludedTerm int
	var MaxLogIndex int
	var CommitIndex int
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.CurrentTerm = 0
		rf.VoteFor = -1
		rf.Logs = make(map[int]LogEntry)
		return
	}
	// Your code here (2C).
	// Example:
	labgob.Register(map[int]LogEntry{})
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&FirstUnsavedIndex) != nil ||
		d.Decode(&LastIncludedTerm) != nil ||
		d.Decode(&MaxLogIndex) != nil ||
		d.Decode(&CommitIndex) != nil {
		DPrintf("Error: Fail to load status from exiting persisted states.")
	} else {
		DPrintf("Server %d Status Loaded, CurrentTerm %d, voteFor %d, rf.FirstUnsavedIndex %d", rf.me, currentTerm, voteFor, FirstUnsavedIndex)
		DPrintf("Server %d logs is %v", rf.me, logs)
		rf.CurrentTerm = currentTerm
		rf.VoteFor = voteFor
		rf.Logs = logs
		rf.FirstUnsavedIndex = FirstUnsavedIndex
		rf.LastIncludedTerm = LastIncludedTerm
		rf.MaxLogIndex = MaxLogIndex
		rf.CommitIndex = CommitIndex
		for i := 0; i < rf.n; i++ {
			rf.NextIndex[i] = rf.MaxLogIndex + 1
			DPrintf("Server %d update rf.NextIndex[%d] %d", rf.me, i, rf.NextIndex[i])
		}
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	candidateTerm := args.Term
	candidateId := args.CandidateId

	// Reply false if term < currentTerm
	DPrintf("Server %d is requested by %d for a vote,  Server %d at Term %d, and votefor is %d", rf.me, candidateId, candidateId, candidateTerm, rf.VoteFor)
	reply.Term = rf.CurrentTerm

	if rf.CurrentTerm > candidateTerm ||
		(rf.CurrentTerm == candidateTerm && rf.VoteFor != -1 && rf.VoteFor != candidateId) ||
		// In 2B need to guarantee that candidate’s log is at least as up-to-date as receiver’s log
		(rf.MaxLogIndex > 0 && rf.Logs[rf.MaxLogIndex].Term > args.LastLogTerm) ||
		rf.MaxLogIndex > 0 && (rf.Logs[rf.MaxLogIndex].Term == args.LastLogTerm && rf.MaxLogIndex > args.LastLogIndex) {
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		DPrintf("Server %d do not vote for %d, len(rf.logs) is %d, args.LastLogTerm is %d, args.LastLogIndex is %d",
			rf.me, candidateId, rf.MaxLogIndex, args.LastLogTerm, args.LastLogIndex)

		if rf.CurrentTerm < candidateTerm {
			rf.CurrentTerm = candidateTerm
			rf.Status = Follower
			rf.persist()
		}

		return
	}

	reply.VoteGranted = true
	rf.CurrentTerm = candidateTerm
	rf.Status = Follower
	rf.VoteFor = candidateId
	rf.persist()
	rf.election_timer.Reset(RandDuringGenerating(election_wait_l, election_wait_r) * time.Millisecond)
	DPrintf("Server %d vote for %d", rf.me, candidateId)

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntries RPC related
type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type AppendEntryReply struct {
	Term                   int
	Id                     int
	LastIndex              int
	Success                bool
	FirstIndexOfFailedTerm int
	FailedTerm             int
}

func (rf *Raft) GetMinIndexOfTerm(term int) int {
	DPrintf("Server %d GetMinIndexOfTerm, term is %d, rf.FirstUnsavedIndex %d, rf.MaxLogIndex %d",
		rf.me, term, rf.FirstUnsavedIndex, rf.MaxLogIndex)
	DPrintf("logs is %v", rf.Logs)
	minIndex := rf.FirstUnsavedIndex - 1
	for i := rf.MaxLogIndex; i > rf.FirstUnsavedIndex-1; i-- {
		DPrintf("rf.Logs[i]: %v", rf.Logs[i])
		if log, ok := rf.Logs[i]; ok && log.Term < term {
			minIndex = i + 1
			break
		}
	}
	return minIndex
}

func (rf *Raft) checkOutOfOrder(args *AppendEntryArgs) bool {
	argsLastIndex := args.PrevLogIndex + len(args.Entries)
	lastIndex := rf.MaxLogIndex
	lastTerm := 0
	if rf.MaxLogIndex < rf.FirstUnsavedIndex {
		lastTerm = rf.LastIncludedTerm
	} else {
		lastTerm = rf.Logs[rf.MaxLogIndex].Term
	}
	if argsLastIndex < lastIndex && lastTerm == args.Term {
		DPrintf("Server %d checkOutOfOrder, argsLastIndex %d, lastIndex %d, lastTerm %d, args.Term %d", rf.me, argsLastIndex, lastIndex, lastTerm, args.Term)
		return true
	}
	return false
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	leaderTerm := args.Term
	reply.Id = rf.me
	// leaderId := args.LeaderId
	DPrintf("Server %d AppendEntries, args.Term is %d, rf.CurrentTerm is %d",
		rf.me, args.Term, rf.CurrentTerm)

	reply.Term = rf.CurrentTerm
	if leaderTerm < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.LastIndex = rf.MaxLogIndex
		reply.Success = false
		reply.FailedTerm = 0
		reply.FirstIndexOfFailedTerm = 0
		return
	}

	if leaderTerm > rf.CurrentTerm {
		rf.CurrentTerm = leaderTerm

		// in case it's a candidate or old leader
		rf.Status = Follower
		rf.persist()
	}

	// 2B append logs
	// Check: Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	reqPrevLogIndex := args.PrevLogIndex
	reqPrevLogTerm := args.PrevLogTerm

	DPrintf("Server %d reqPrevLogIndex %d, rf.FirstUnsavedIndex %d, reqPrevLogTerm %d, rf.LastIncludedTerm %d", rf.me, reqPrevLogIndex, rf.FirstUnsavedIndex, reqPrevLogTerm, rf.LastIncludedTerm)
	if reqPrevLogIndex == 0 {
	} else if reqPrevLogIndex == rf.FirstUnsavedIndex-1 {
		if rf.checkOutOfOrder(args) {
			reply.Term = rf.CurrentTerm
			reply.LastIndex = rf.MaxLogIndex
			reply.Success = false
			// By the hint from 6.824, we search for the first index of conflicting Term
			reply.FailedTerm = 0
			reply.FirstIndexOfFailedTerm = 0
			return
		}
		if reqPrevLogTerm != rf.LastIncludedTerm {
			reply.Term = rf.CurrentTerm
			reply.LastIndex = rf.MaxLogIndex
			reply.Success = false
			// By the hint from 6.824, we search for the first index of conflicting Term
			reply.FailedTerm = rf.LastIncludedTerm
			reply.FirstIndexOfFailedTerm = 0
			return
		}
	} else if reqPrevLogIndex < rf.FirstUnsavedIndex-1 {
		// Already been applied
		reply.Term = rf.CurrentTerm
		reply.LastIndex = rf.MaxLogIndex
		reply.Success = false
		reply.FailedTerm = rf.LastIncludedTerm
		reply.FirstIndexOfFailedTerm = 0
		return
	} else {
		if rf.checkOutOfOrder(args) {
			reply.Term = rf.CurrentTerm
			reply.LastIndex = rf.MaxLogIndex
			reply.Success = false
			// By the hint from 6.824, we search for the first index of conflicting Term
			reply.FailedTerm = 0
			reply.FirstIndexOfFailedTerm = 0
			return
		}

		checkedLog, ok := rf.Logs[reqPrevLogIndex]
		DPrintf("reqPrevLogIndex %d, ok %v, rf.MaxLogIndex %d, reqPrevLogTerm %d, checkedLog.Term %d", reqPrevLogIndex, ok, rf.MaxLogIndex, reqPrevLogTerm, checkedLog.Term)
		if !ok || (ok && checkedLog.Term != reqPrevLogTerm) {
			reply.Term = rf.CurrentTerm
			reply.LastIndex = rf.MaxLogIndex
			reply.Success = false

			// By the hint from 6.824, we search for the first index of conflicting Term
			if reqPrevLogIndex != 0 && !ok {
				reply.FailedTerm = Max(rf.Logs[rf.MaxLogIndex].Term, 1)
			} else {
				reply.FailedTerm = checkedLog.Term
			}
			reply.FirstIndexOfFailedTerm = rf.GetMinIndexOfTerm(reply.FailedTerm)
			return
		}
	}

	if _, ok := rf.Logs[reqPrevLogIndex+1]; ok {
		DPrintf("Server %d has existing entry at index %d (value %+v) conflicts with a new one", rf.me, reqPrevLogIndex+1, rf.Logs[reqPrevLogIndex+1])
		for i := rf.MaxLogIndex; i > reqPrevLogIndex; i-- {
			DPrintf("Delete server %d Log pos at %d", rf.me, i)
			delete(rf.Logs, i)
			rf.MaxLogIndex -= 1
		}
	}

	// TODO: For Now Assume there only contains one entry in the list

	iter := reqPrevLogIndex + 1
	for _, entry := range args.Entries {
		DPrintf("Server %d append %d entry %v", rf.me, iter, entry)
		rf.Logs[iter] = entry
		rf.MaxLogIndex = iter
		iter += 1
	}

	if args.LeaderCommit > rf.CommitIndex {
		DPrintf("Server %d len(rf.notifyCh) is %d, rf.CommitIndex %d, args.LeaderCommit %d", rf.me, len(rf.notifyCh), rf.CommitIndex, args.LeaderCommit)
		rf.CommitIndex = Min(args.LeaderCommit, rf.MaxLogIndex)
		if len(rf.notifyCh) == 0 {
			rf.notifyCh <- struct{}{}
		}
	}

	reply.Success = true
	DPrintf("Server %d show rf.Logs length is %d", rf.me, rf.MaxLogIndex)
	reply.LastIndex = rf.MaxLogIndex
	rf.Status = Follower
	rf.persist()
	rf.election_timer.Reset(RandDuringGenerating(election_wait_l, election_wait_r) * time.Millisecond)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.Status != Leader {
		isLeader = false
		return -1, term, isLeader
	}

	term = rf.CurrentTerm
	rf.MaxLogIndex += 1
	// Append itself
	rf.Logs[rf.MaxLogIndex] = LogEntry{term, command}
	rf.sendHeartBeat()
	rf.persist()

	return rf.MaxLogIndex, term, isLeader
}

func (rf *Raft) sendAEs(curTerm int, commitIndex int) chan AppendEntryReply {
	aerChan := make(chan AppendEntryReply)

	for i := 0; i < rf.n; i++ {
		if i != rf.me {
			var aer AppendEntryReply
			// send snapshots
			if rf.FirstUnsavedIndex > rf.NextIndex[i] {
				// if false {
				DPrintf("Server %d to server %d rf.FirstUnsavedIndex > rf.NextIndex[i]: %d vs %d", rf.me, i, rf.FirstUnsavedIndex, rf.NextIndex[i])
				LastIncludedIndex := rf.FirstUnsavedIndex - 1
				LastIncludedTerm := rf.LastIncludedTerm
				data := rf.persister.ReadSnapshot()
				go func(i int, ch chan AppendEntryReply) {
					rf.sendSnapshot(i,
						&InstallSnapshotArgs{
							curTerm,
							rf.me,
							LastIncludedIndex,
							LastIncludedTerm,
							data},
						&aer)
					DPrintf("Server %d send snapshot to server %d, rf.FirstUnsavedIndex > rf.NextIndex[i], aer is %+v", rf.me, i, aer)
					ch <- aer
				}(i, aerChan)

			} else {
				DPrintf("Server %d to server %d rf.FirstUnsavedIndex <= rf.NextIndex[i]: %d vs %d", rf.me, i, rf.FirstUnsavedIndex, rf.NextIndex[i])
				logs := []LogEntry{}

				nextIndex := rf.NextIndex[i]
				lastLogIndex := rf.MaxLogIndex

				if nextIndex <= lastLogIndex {
					for i := nextIndex; i < lastLogIndex+1; i++ {
						logs = append(logs, rf.Logs[i])
					}
				}

				preLogIndex := nextIndex - 1
				preLogTerm := 0
				DPrintf("Server %d to server %d preLogIndex %d, rf.LastIncludedTerm %d, FirstUnsavedIndex %d, logs %v", rf.me, i, preLogIndex, rf.LastIncludedTerm, rf.FirstUnsavedIndex, rf.Logs)
				if preLogIndex >= rf.FirstUnsavedIndex {
					preLogTerm = rf.Logs[preLogIndex].Term
				} else {
					preLogTerm = rf.LastIncludedTerm
				}
				go func(i int, ch chan AppendEntryReply) {

					rf.sendAppendEntries(i,
						&AppendEntryArgs{curTerm, rf.me, preLogIndex, preLogTerm, logs, commitIndex},
						&aer)
					DPrintf("Server %d send logentry to server %d, rf.FirstUnsavedIndex <= rf.NextIndex[i], aer is %+v", rf.me, i, aer)
					ch <- aer

				}(i, aerChan)
			}

		}
	}
	return aerChan
}

func (rf *Raft) checkApplies() bool {
	// Update CommitedIndex
	flag := false
	preCommitedIndex := rf.CommitIndex
	for i := rf.MaxLogIndex; i >= rf.CommitIndex; i-- {
		committedNum := 0
		for j := 0; j < rf.n; j++ {
			if rf.MatchIndex[j] >= i {
				committedNum += 1
			}
		}
		if committedNum >= int(rf.n/2) {
			preCommitedIndex = i
			break
		}
	}

	// check if this log occurs in this term
	DPrintf("Leader %d, rf.CommitIndex %d, preCommitedIndex %d", rf.me, rf.CommitIndex, preCommitedIndex)
	if rf.Logs[preCommitedIndex].Term == rf.CurrentTerm {
		flag = (rf.CommitIndex < preCommitedIndex)
		rf.CommitIndex = preCommitedIndex
		rf.persist()
	}
	return flag
}

func (rf *Raft) checkCommitted(aerChan chan AppendEntryReply) {
	var aer AppendEntryReply
	success := 0
Wait_Reply_Loop:
	for i := 0; i < rf.n-1; i++ {
		select {
		case aer = <-aerChan:
			rf.mu.Lock()
			appendCurTerm := aer.Term
			appendSuccess := aer.Success
			appendIndex := aer.LastIndex
			appendId := aer.Id
			DPrintf("Leader %d already send appendentry to server %d, answer is %+v", rf.me, aer.Id, aer)

			if !appendSuccess {
				if rf.CurrentTerm < appendCurTerm {
					DPrintf(`Leader %d got a msg Term bigger from %d than itself,
								for currentTerm is %d, appendCurTerm is %d`, rf.me, appendId, rf.CurrentTerm, appendCurTerm)
					rf.CurrentTerm = appendCurTerm
					rf.Status = Follower
					rf.persist()
					rf.mu.Unlock()
					break Wait_Reply_Loop

				}

				// log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
				if aer.FailedTerm != 0 {
					rf.NextIndex[appendId] = Max(1, aer.FirstIndexOfFailedTerm)
					DPrintf("Leader failed %d update Server %d status: MatchIndex %d", rf.me, appendId, rf.NextIndex[appendId])
				}

			} else {
				success += 1
				rf.NextIndex[appendId] = appendIndex + 1
				rf.MatchIndex[appendId] = appendIndex
				DPrintf("Leader %d update Server %d status: MatchIndex %d", rf.me, appendId, appendIndex)
				DPrintf("Leader %d at i %d appendIndex %d, rf.CommitIndex %d", rf.me, i, appendIndex, rf.CommitIndex)

				// trigger apply manually

				if success == rf.n-1 {
					DPrintf("Leader %d trigger apply_timer.Reset(0), rf.CommitIndex %d, appendIndex %d", rf.me, rf.CommitIndex, appendIndex)
					DPrintf("Server %d len(rf.notifyCh) is %d", rf.me, len(rf.notifyCh))
					if len(rf.notifyCh) == 0 {
						rf.notifyCh <- struct{}{}
					}
				}

			}

			rf.mu.Unlock()
		case <-time.After(200 * time.Millisecond):
			DPrintf("checkcommited time out")
			if rf.killed() {
				return
			}
			break Wait_Reply_Loop
		}
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.stopCh <- true
	rf.stopCh <- true
	rf.stopCh <- true
	close(rf.stopCh)
	DPrintf("Server %d was killed", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) holdingElection() {
	for {
		select {
		case <-rf.stopCh:
			DPrintf("Server %d stop holding election", rf.me)
			return
		case <-rf.election_timer.C:
			DPrintf("Server %d election timesout, start a election.", rf.me)
			// Start an Election

			rf.mu.Lock()
			rf.CurrentTerm += 1
			CurrentTerm := rf.CurrentTerm
			rf.VoteFor = rf.me
			rf.Status = Candidate
			rf.persist()

			votesCount := 1
			voteCh := make(chan RequestVoteReply)

			// for when you
			for i := 0; i < rf.n; i++ {
				maxLogIndex := rf.MaxLogIndex
				maxLogTerm := rf.Logs[rf.MaxLogIndex].Term

				if i != rf.me {

					go func(i int, ch chan RequestVoteReply) {
						var rvr RequestVoteReply
						rf.sendRequestVote(i, &RequestVoteArgs{CurrentTerm, rf.me, maxLogIndex,
							maxLogTerm}, &rvr)
						voteCh <- rvr
					}(i, voteCh)

				}
			}

			var rvr RequestVoteReply

		Wait_Reply_Loop:
			for i := 0; i < rf.n-1; i++ {
				select {
				case rvr = <-voteCh:
					voteCurTerm := rvr.Term
					voteGranted := rvr.VoteGranted
					// Process them
					DPrintf("Server %d got vote RequesetVoteReply%+v", rf.me, rvr)

					if voteCurTerm > rf.CurrentTerm {
						rf.Status = Follower
						rf.CurrentTerm = voteCurTerm
						rf.persist()
						break Wait_Reply_Loop
					}

					if voteGranted {
						votesCount += 1
					}

				case <-time.After(200 * time.Millisecond):
					if rf.killed() {
						rf.mu.Unlock()
						return
					}
					break Wait_Reply_Loop
				}
			}

			// Become a leader
			DPrintf("Server %d got votes for %d at Term %d", rf.me, votesCount, rf.CurrentTerm)
			if votesCount > int(rf.n/2) && rf.Status == Candidate {
				DPrintf("Server %d wins leadership", rf.me)
				rf.Status = Leader

				for i := 0; i < rf.n; i++ {
					rf.NextIndex[i] = rf.MaxLogIndex + 1
					DPrintf("Leader %d update rf.NextIndex[%d] %d", rf.me, i, rf.NextIndex[i])
					rf.MatchIndex[i] = 0
				}
			}
			rf.mu.Unlock()

			for {
				rf.mu.Lock()
				if rf.Status != Leader || rf.killed() {
					rf.mu.Unlock()
					break
				}
				rf.mu.Unlock()
				DPrintf("Leader %d at term %d block to be waiting as a follower", rf.me, rf.CurrentTerm)
				time.Sleep(100 * time.Millisecond)
			}
			rf.election_timer.Reset(RandDuringGenerating(election_wait_l, election_wait_r) * time.Millisecond)
		}
	}
}

func (rf *Raft) trySendHeartBeat() {
	for {
		// Block when not a leader
		select {
		case <-rf.stopCh:
			DPrintf("Server %d stop sending heartbeats", rf.me)
			return
		case <-time.After(100 * time.Millisecond):
			rf.mu.Lock()

			if rf.Status != Leader {
				rf.mu.Unlock()
				time.Sleep(100 * time.Millisecond)
				continue
			}

			rf.sendHeartBeat()
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) sendHeartBeat() {
	DPrintf("Leader %d start sending heartbeats.", rf.me)

	curTerm := rf.CurrentTerm
	commitIndex := rf.CommitIndex

	// Send AppendEntry
	aerChan := rf.sendAEs(curTerm, commitIndex)
	go rf.checkCommitted(aerChan)

}

func (rf *Raft) applyMsg() {
	for {
		select {
		case <-rf.stopCh:
			DPrintf("Server %d stop applying msgs", rf.me)
			return
		case <-rf.apply_timer.C:
			if len(rf.notifyCh) > 0 {
				rf.apply_timer.Reset(100 * time.Millisecond)
				continue
			}
			rf.notifyCh <- struct{}{}
		case <-rf.notifyCh:
			rf.sendMsg()
			rf.apply_timer.Reset(100 * time.Millisecond)
		}
	}
}

func (rf *Raft) sendMsg() {
	rf.mu.Lock()
	rf.checkApplies()
	msgs := make([]ApplyMsg, 0)
	DPrintf("Leader %d LastApplied %d, FirstUnsavedIndex %d, CommitIndex %d, logs %v", rf.me, rf.LastApplied, rf.FirstUnsavedIndex, rf.CommitIndex, rf.Logs)
	if rf.LastApplied < rf.FirstUnsavedIndex-1 {
		msgs = append(msgs, ApplyMsg{
			CommandValid: false,
			Command:      "installSnapShot",
			CommandIndex: rf.FirstUnsavedIndex - 1,
		})
	} else if rf.CommitIndex > rf.LastApplied {
		for i := rf.LastApplied + 1; i <= rf.CommitIndex; i++ {
			msgs = append(msgs, ApplyMsg{
				CommandValid: true,
				Command:      rf.Logs[i].Command,
				CommandIndex: i,
			})
		}
	}
	rf.mu.Unlock()

	for _, msg := range msgs {
		rf.applyCh <- msg
		DPrintf("Server %d apply msg %+v", rf.me, msg)
		rf.mu.Lock()
		rf.LastApplied = msg.CommandIndex
		rf.mu.Unlock()
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	labgob.Register(map[int]LogEntry{})

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.n = len(peers)
	rf.applyCh = applyCh
	rf.stopCh = make(chan bool, 1)

	// Your initialization code here (2A, 2B, 2C).

	// 2A: To implement most of the functions a raft serve must hold
	rf.Status = Follower
	// rf.CurrentTerm = 0
	// rf.VoteFor = -1

	// 2B: Init Log apply and commit index
	rf.CommitIndex = 0
	// rf.Logs = make(map[int]LogEntry)
	rf.NextIndex = make([]int, rf.n)
	for i := 0; i < rf.n; i++ {
		rf.NextIndex[i] = 1
	}
	rf.MatchIndex = make([]int, rf.n)

	// 3B: Init Snapshot
	rf.FirstUnsavedIndex = 1
	rf.MaxLogIndex = 0
	rf.LastIncludedTerm = 0

	rf.election_timer = time.NewTimer(RandDuringGenerating(election_wait_l, election_wait_r) * time.Millisecond)
	rf.apply_timer = time.NewTimer(100 * time.Millisecond)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.LastApplied = rf.FirstUnsavedIndex - 1
	rf.notifyCh = make(chan struct{}, 100)

	DPrintf(" Server %d init", rf.me)

	// Election Management
	go rf.holdingElection()

	// AppendEntries Function
	go rf.trySendHeartBeat()

	go rf.applyMsg()

	return rf
}
