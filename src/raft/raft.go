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
	"sync"
	"sync/atomic"
	"time"

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
	Log            []map[int]interface{}
	LeaderNow      int
	election_timer *time.Timer

	// Volatile state on all servers
	CommitIndex int
	LastApplied int
	Status      StatusType

	// Volatile state on leaders
	NextIndex  []int
	MatchIndex []int
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
	if rf.CurrentTerm > candidateTerm {
		reply.VoteGranted = false
		rf.CurrentTerm = candidateTerm
		rf.Status = Follower
		return
	}

	// In 2B need to guarantee that candidate’s log is at least as up-to-date as receiver’s log
	if rf.CurrentTerm == candidateTerm && rf.VoteFor != -1 && rf.VoteFor != candidateId {
		reply.VoteGranted = false
		return
	}

	reply.VoteGranted = true
	rf.CurrentTerm = candidateTerm
	rf.Status = Follower
	rf.VoteFor = candidateId
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
	Entries      []interface{}
	LeaderCommit int
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	leaderTerm := args.Term
	// leaderId := args.LeaderId

	reply.Term = rf.CurrentTerm
	if leaderTerm < rf.CurrentTerm {
		reply.Success = false
		return
	}

	if leaderTerm > rf.CurrentTerm {
		rf.CurrentTerm = leaderTerm

		// in case it's a candidate or old leader
		rf.Status = Follower
	}

	rf.election_timer.Reset(RandDuringGenerating(election_wait_l, election_wait_r) * time.Millisecond)

	reply.Success = true
	// 2B append logs

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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return index, term, isLeader
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) holdingElection(t *time.Timer, c *sync.Cond) {
	for {
		<-t.C
		DPrintf("Server %d election timesout, start a election.", rf.me)
		// Start an Election

		c.L.Lock()
		rf.CurrentTerm += 1
		CurrentTerm := rf.CurrentTerm

		rf.VoteFor = rf.me
		rf.Status = Candidate

		votesCount := 1
		voteCh := make(chan RequestVoteReply)
		c.L.Unlock()

		// for when you
		for i := 0; i < len(rf.peers); i++ {

			if i != rf.me {

				go func(i int, ch chan RequestVoteReply) {
					var rvr RequestVoteReply
					rf.sendRequestVote(i, &RequestVoteArgs{CurrentTerm, rf.me, -1, -1}, &rvr)
					voteCh <- rvr
				}(i, voteCh)

			}
		}

		var rvr RequestVoteReply

	Wait_Reply_Loop:
		for i := 0; i < len(rf.peers); i++ {
			select {
			case rvr = <-voteCh:
				c.L.Lock()
				voteCurTerm := rvr.Term
				voteGranted := rvr.VoteGranted
				// Process them
				DPrintf("Server %d got vote RequesetVoteReply%v", rf.me, rvr)

				if voteCurTerm > rf.CurrentTerm {
					rf.Status = Follower
					rf.CurrentTerm = voteCurTerm
					c.L.Unlock()
					break Wait_Reply_Loop
				}

				if voteGranted {
					votesCount += 1
				}

				c.L.Unlock()

			case <-time.After(200 * time.Millisecond):
				break Wait_Reply_Loop
			}
		}

		// Become a leader
		DPrintf("Server %d got votes for %d at Term %d", rf.me, votesCount, rf.CurrentTerm)
		c.L.Lock()
		if votesCount > int(rf.n/2) && rf.Status == Candidate {
			DPrintf("Server %d wins leadership", rf.me)
			rf.Status = Leader
		}
		c.L.Unlock()
		c.Broadcast()

		for rf.Status == Leader {
			DPrintf("Leader %d block to be waiting as a follower", rf.me)
			time.Sleep(100 * time.Millisecond)
		}
		t.Reset(RandDuringGenerating(election_wait_l, election_wait_r) * time.Millisecond)
	}
}

func (rf *Raft) trySendHeartBeat(c *sync.Cond) {
	for {
		// Block when not a leader
		c.L.Lock()
		for rf.Status != Leader {
			DPrintf("AppendEntries: Server %d wins cond lock, status is %d", rf.me, rf.Status)
			c.Wait()
			// c.L.Unlock()
			// time.Sleep(100 * time.Millisecond)
			// c.L.Lock()
		}
		DPrintf("Leader %d start sending heartbeats.", rf.me)

		aerChan := make(chan AppendEntryReply)
		currentTerm := rf.CurrentTerm
		c.L.Unlock()

		// Send AppendEntry
		for i := 0; i < len(rf.peers); i++ {
			// t.Reset(RandDuringGenerating(election_wait_l, election_wait_r) * time.Millisecond)
			if i != rf.me {

				go func(i int, ch chan AppendEntryReply) {
					var aer AppendEntryReply
					rf.sendAppendEntries(i,
						&AppendEntryArgs{currentTerm, rf.me, -1, -1, make([]interface{}, 0), -1},
						&aer)
					ch <- aer

				}(i, aerChan)
			}
		}

		var aer AppendEntryReply
	Wait_Reply_Loop:
		for i := 0; i < len(rf.peers); i++ {
			select {
			case aer = <-aerChan:
				c.L.Lock()
				DPrintf("Leader %d already send appendentry to server %d", rf.me, i)
				appendCurTerm := aer.Term
				appendSuccess := aer.Success

				if !appendSuccess {
					if rf.CurrentTerm < appendCurTerm {
						DPrintf(`Leader %d got a msg Term bigger than itself,
								for currentTerm is %d, appendCurTerm is %d`, rf.me, rf.CurrentTerm, appendCurTerm)
						rf.CurrentTerm = appendCurTerm
						rf.Status = Follower
						c.L.Unlock()
						break Wait_Reply_Loop
					}
				}

				c.L.Unlock()
			case <-time.After(200 * time.Millisecond):
				break Wait_Reply_Loop
			}
		}

		time.Sleep(100 * time.Millisecond)
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.n = len(peers)

	// Your initialization code here (2A, 2B, 2C).

	// 2A: To implement most of the functions a raft serve must hold
	rf.Status = Follower
	rf.LeaderNow = -1 // Unknow
	rf.CurrentTerm = 0
	rf.VoteFor = -1

	election_cond := sync.NewCond(&rf.mu)
	rf.election_timer = time.NewTimer(RandDuringGenerating(election_wait_l, election_wait_r) * time.Millisecond)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	DPrintf(" Server %d init", rf.me)

	// Election Management
	go rf.holdingElection(rf.election_timer, election_cond)

	// AppendEntries Function
	go rf.trySendHeartBeat(election_cond)

	return rf
}
