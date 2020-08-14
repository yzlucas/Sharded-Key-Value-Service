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
	"../labrpc"
	"bytes"
	"encoding/gob"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	RoleFollower  = 0
	RoleCandidate = 1
	RoleLeader    = 2

	heartbeatIntervalMin = 200
	heartbeatIntervalMax = 500

	electionTimeoutMin = 200
	electionTimeoutMax = 500

	LeaderHeartbeat = time.Millisecond * 120
)

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

type LogEntry struct {
	Term int
	Data interface{}
}

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

	role int

	voteCount int

	lastApplied int
	commitIndex int

	nextIndex  []int
	matchIndex []int

	heartbeatCh   chan int
	electWinCh    chan int
	grantedVoteCh chan int
	applyCh       chan ApplyMsg

	// persistent state
	voteFor int
	log     []LogEntry
	term    int
}

func (rf *Raft) unsafeGetLastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) unsafeGetLastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.term, rf.role == RoleLeader
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

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) unsafeIsLatest(candidateIndex, candidateTerm int) bool {
	DPrintf("[%d] logTerm = %d, logIndex = %d, candidateTerm = %d, candidateIndex = %d \n",
		rf.me, rf.unsafeGetLastLogTerm(), rf.unsafeGetLastLogIndex(), candidateTerm, candidateIndex)

	if rf.unsafeGetLastLogTerm() < candidateTerm {
		return true
	}

	return rf.unsafeGetLastLogTerm() == candidateTerm && rf.unsafeGetLastLogIndex() <= candidateIndex
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

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.term)
	d.Decode(&rf.voteFor)
	d.Decode(&rf.log)
}

func (rf *Raft) generateElectionTimeout() time.Duration {
	rand.Seed(time.Now().Unix()*10 + int64(rf.me))

	randomSecond := rand.Intn(electionTimeoutMax - electionTimeoutMin)
	return time.Duration(electionTimeoutMin+randomSecond) * time.Millisecond
}

func (rf *Raft) generateHeartbeatInterval() time.Duration {
	rand.Seed(time.Now().Unix()*10 + int64(rf.me))

	randomSecond := rand.Intn(heartbeatIntervalMax - heartbeatIntervalMin)
	return time.Duration(heartbeatIntervalMin+randomSecond) * time.Millisecond
}

func (rf *Raft) unsafeChangeRole(newRole int) {
	rf.role = newRole
}

type AppendEntriesArgs struct {
	Term             int
	Master           int
	Entries          []LogEntry
	PreviousLogIndex int
	PreviousLogTerm  int
	LeaderCommit     int
}

type AppendEntriesReply struct {
	Term         int
	Ack          bool
	NextTryIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Ack = false
	if args.Term < rf.term {
		reply.Term = rf.term
		reply.Ack = false
		reply.NextTryIndex = rf.unsafeGetLastLogIndex() + 1
		return
	}

	rf.heartbeatCh <- rf.term
	if args.Term > rf.term {
		rf.term = args.Term
		rf.unsafeChangeRole(RoleFollower)
		rf.voteFor = -1
	}

	reply.Term = rf.term

	if args.PreviousLogIndex > rf.unsafeGetLastLogIndex() {
		DPrintf("[%d] previous log too high, current = %d, heartbeat = %d \n",
			rf.me, rf.unsafeGetLastLogIndex(), args.PreviousLogIndex)

		reply.NextTryIndex = rf.unsafeGetLastLogIndex() + 1
		return
	}

	if args.PreviousLogIndex > 0 && rf.log[args.PreviousLogIndex].Term != args.PreviousLogTerm {
		logTerm := rf.log[args.PreviousLogIndex].Term
		for i := args.PreviousLogIndex - 1; i > 0; i-- {
			if rf.log[i].Term != logTerm {
				reply.NextTryIndex = i + 1
				break
			}
		}

		DPrintf("[%d] previous log conflict, argTerm = %d, currentTerm = %d, currentIndex = %d, heartbeatIndex = %d, nextTry = %d \n",
			rf.me, args.Term, rf.log[args.PreviousLogIndex].Term, rf.unsafeGetLastLogIndex(), args.PreviousLogIndex, reply.NextTryIndex)
	} else {
		newLog := rf.log[:args.PreviousLogIndex+1]
		rf.log = append(newLog, args.Entries...)

		reply.Ack = true
		reply.NextTryIndex = args.PreviousLogIndex

		//DPrintf("[%d] leaderCommit = %d, currentCommit = %d \n", rf.me, args.LeaderCommit, rf.commitIndex)
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit <= rf.unsafeGetLastLogIndex() {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = rf.unsafeGetLastLogIndex()
			}

			go rf.commit()
		}
	}
}

func (rf *Raft) sendAllAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me && rf.role == RoleLeader {
			args := &AppendEntriesArgs{
				Term:             rf.term,
				Master:           rf.me,
				PreviousLogIndex: rf.nextIndex[i] - 1,
				LeaderCommit:     rf.commitIndex,
			}

			if args.PreviousLogIndex >= 0 {
				args.PreviousLogTerm = rf.unsafeGetLastLogTerm()
			}

			if rf.nextIndex[i] <= rf.unsafeGetLastLogIndex() {
				args.Entries = rf.log[rf.nextIndex[i]:]
			}

			go rf.sendAppendEntries(i, args, &AppendEntriesReply{})
		}
	}
}

func (rf *Raft) startElect() {
	DPrintf("[%d] Start Elect, term = %d \n", rf.me, rf.term)

	rf.mu.Lock()
	rf.term++
	rf.voteCount = 1
	rf.voteFor = rf.me
	args := &RequestVoteArgs{
		Id:           rf.me,
		Term:         rf.term,
		LastLogTerm:  rf.unsafeGetLastLogTerm(),
		LastLogIndex: rf.unsafeGetLastLogIndex(),
	}
	rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me && rf.role == RoleCandidate {
			go rf.sendRequestVote(i, args, &RequestVoteReply{})
		}
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Id           int
	Term         int
	LastLogTerm  int
	LastLogIndex int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term    int
	Granted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	DPrintf("[%d] received request vote %v \n", rf.me, *args)

	if args.Term < rf.term {
		DPrintf("[%d] received request vote request, not granted because of low term \n", rf.me)
		reply.Granted = false
		reply.Term = rf.term
		return
	}

	if args.Term > rf.term {
		DPrintf("[%d] received request vote request, term is higher than now \n", rf.me)
		rf.term = args.Term
		rf.unsafeChangeRole(RoleFollower)
		rf.voteFor = -1
	}

	if (rf.voteFor == -1 || rf.voteFor == args.Id) && rf.unsafeIsLatest(args.LastLogIndex, args.LastLogTerm) {
		DPrintf("[%d] granted vote for %d \n", rf.me, args.Id)
		reply.Granted = true
		rf.voteFor = args.Id
		rf.unsafeChangeRole(RoleFollower)
		rf.grantedVoteCh <- args.Term
	} else {
		DPrintf("[%d] vote fail for %d, currentVoteFor = %d, isLatest = %v \n",
			rf.me, args.Id, rf.voteFor, rf.unsafeIsLatest(args.LastLogIndex, args.LastLogTerm))

		reply.Granted = false
		rf.voteFor = -1
	}
	reply.Term = rf.term
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
	DPrintf("[%d] Sent request vote rpc to server %d \n", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	if !ok {
		return ok
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != RoleCandidate || rf.term != args.Term {
		return ok
	}

	if reply.Term > rf.term {
		rf.unsafeChangeRole(RoleFollower)
		rf.term = reply.Term
		DPrintf("[%d] Term updated to = %d \n", rf.me, rf.term)
		rf.voteFor = -1
		rf.persist()

		return ok
	}

	if reply.Granted {
		rf.voteCount++
		if rf.voteCount > len(rf.peers)/2 {
			DPrintf("[%d] Vote success, now leader \n", rf.me)
			rf.unsafeChangeRole(RoleLeader)
			rf.electWinCh <- rf.term
		}
	}

	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//DPrintf("[%d] Sending hearbeat to %d \n", rf.me, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if !ok {
		return ok
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != RoleLeader || rf.term != args.Term {
		return ok
	}

	if reply.Term > rf.term {
		rf.unsafeChangeRole(RoleFollower)
		rf.term = reply.Term
		rf.voteFor = -1
		rf.persist()
		return ok
	}

	if reply.Ack {
		rf.matchIndex[server] = args.PreviousLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else {
		rf.nextIndex[server] = reply.NextTryIndex
	}

	for i := rf.unsafeGetLastLogIndex(); i > rf.commitIndex; i-- {
		cnt := 1

		for peerId := range rf.peers {
			if peerId != rf.me && rf.matchIndex[peerId] >= i && rf.log[i].Term == rf.term {
				cnt++
			}
		}

		if cnt > len(rf.peers)/2 {
			rf.commitIndex = i
			go rf.commit()
			break
		}
	}
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
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := rf.role == RoleLeader

	if isLeader {
		index = rf.unsafeGetLastLogIndex() + 1
		term = rf.term
		rf.log = append(rf.log, LogEntry{
			Term: term,
			Data: command,
		})
		rf.persist()
	}

	DPrintf("[%d] Start index = %d, term = %d, isLeader = %v \n", rf.me, index, term, isLeader)
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

func (rf *Raft) run() {
	for {
		switch rf.role {
		case RoleLeader:
			//DPrintf("[%d] is Leader, run leader loop \n", rf.me)
			rf.sendAllAppendEntries()
			time.Sleep(LeaderHeartbeat)
		case RoleFollower:
			//DPrintf("[%d] is Follower, run follower loop \n", rf.me)
			select {
			case <-rf.heartbeatCh:
			//DPrintf("[%d] heartbeat package received %d \n", rf.me, pkg)
			case <-rf.grantedVoteCh:
			case <-time.After(rf.generateHeartbeatInterval()):
				rf.mu.Lock()
				rf.unsafeChangeRole(RoleCandidate)
				rf.mu.Unlock()
			}
		case RoleCandidate:
			//DPrintf("[%d] is Candidate, run candidate loop \n", rf.me)
			rf.startElect()
			select {
			case <-rf.heartbeatCh:
				rf.mu.Lock()
				rf.unsafeChangeRole(RoleFollower)
				rf.mu.Unlock()
			case <-rf.electWinCh:
				rf.mu.Lock()
				DPrintf("[%d] Win the elect, term = %d \n", rf.me, rf.term)
				rf.unsafeChangeRole(RoleLeader)

				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))

				for i := range rf.nextIndex {
					rf.nextIndex[i] = rf.unsafeGetLastLogIndex() + 1
					rf.matchIndex[i] = 0
				}

				rf.mu.Unlock()
			case <-time.After(rf.generateElectionTimeout()):
			}
		}
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
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		role:      RoleFollower,

		voteFor:       -1,
		heartbeatCh:   make(chan int, 100),
		electWinCh:    make(chan int, 100),
		grantedVoteCh: make(chan int, 100),

		applyCh: applyCh,
		log: []LogEntry{{
			Term: 0,
			Data: nil,
		}},
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash

	rf.readPersist(persister.ReadRaftState())
	rf.persist()

	go rf.run()
	return rf
}

func (rf *Raft) commit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[%d] Committing logs Start \n", rf.me)
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{
			Command:      rf.log[i].Data,
			CommandIndex: i,
			CommandValid: true,
		}

		rf.lastApplied = i
	}

	DPrintf("[%d] Committing logs End, lastApplied = %d, commitIndex = %d \n",
		rf.me, rf.lastApplied, rf.commitIndex)
}

func (rf *Raft) isLogConflict(server []LogEntry, client []LogEntry) bool {
	for i := range server {
		if i >= len(client) {
			break
		}

		if server[i].Term != client[i].Term {
			return true
		}
	}
	return false
}