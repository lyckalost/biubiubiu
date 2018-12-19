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
	"labgob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

// WorkerState : a enum of work states.
type WorkerState int

// wokerStae Enum
const (
	Candidate WorkerState = 0
	Leader                = 1
	Follower              = 2
)

// LeaderHeartbeatIntervalMilli :
const LeaderHeartbeatIntervalMilli = 50

// FollowerHeartbeatTimeoutMilli : Define Follower heartbeat timeout here.
const FollowerHeartbeatTimeoutMilli = 400

// ElectionTimeoutMilli : Define Election timeout of each term here.
const ElectionTimeoutMilli = 1000

// LogEntry :
type LogEntry struct {
	Term    int
	Command interface{}
}

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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	//2A
	state                    WorkerState
	heatbeatSignalCh         chan bool
	startNewElectionSignalCh chan bool
	winElectionSignalCh      chan bool
	currentTerm              int
	logs                     []LogEntry

	//2B
	nextIndex   []int
	matchIndex  []int
	applyCh     chan ApplyMsg
	commitIndex int
	lastApplied int
	votedFor    int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func Min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func Max(a int, b int) int {
	if a < b {
		return b
	} else {
		return a
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()
	// Your code here (2A).
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	rf.mu.Lock()
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	rf.mu.Unlock()
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	d := labgob.NewDecoder(r)
	var term int
	var votedID int
	var entries []LogEntry
	if d.Decode(&term) != nil ||
		d.Decode(&votedID) != nil ||
		d.Decode(&entries) != nil {
		// got error
	} else {
		rf.mu.Lock()
		rf.currentTerm = term
		rf.votedFor = votedID
		rf.logs = entries
		rf.mu.Unlock()
	}
	// fmt.Printf("Server %d readPersist with term %d logs: %v\n", rf.me, rf.currentTerm, rf.logs)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
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

type AppendEntryArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

func (rf *Raft) applyChangesToLatestCommit() {
	if rf.commitIndex > rf.lastApplied {
		for i := rf.lastApplied + 1 - 1; i <= rf.commitIndex-1; i++ {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[i].Command,
				CommandIndex: i + 1}
		}
		rf.lastApplied = rf.commitIndex
		// fmt.Printf("Applying Server %d with logs: %v\n", rf.me, rf.logs)
	}
	rf.persist()
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	reply.Term = Max(rf.currentTerm, args.Term)
	reply.VoteGranted = false
	if rf.currentTerm <= args.Term {
		// fmt.Printf("Candidate %d with term %d args: %v requesting vote from Server %d with term %d votedFor %d logs: %v\n", args.CandidateID, args.Term, args, rf.me, rf.currentTerm, rf.votedFor, rf.logs)
		// update term implies previous voting are invalid so you can revote for THIS term
		if rf.currentTerm < args.Term {
			rf.votedFor = -1
			rf.currentTerm = args.Term
		}

		// Candidate and has not voted can vote in this term
		if rf.votedFor == -1 {
			// can only given vote if the log term of requestor is at least as update as this, (page 8)
			if logSize := len(rf.logs); logSize == 0 || args.LastLogTerm > rf.logs[logSize-1].Term || (args.LastLogTerm == rf.logs[logSize-1].Term && args.LastLogIndex >= logSize) {
				rf.state = Follower
				reply.VoteGranted = true
			}
		}
	}
	rf.mu.Unlock()
	if reply.VoteGranted == true {
		rf.heatbeatSignalCh <- true
	}
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	// fmt.Printf("Server %d with term %d logs: %v commitIndex: %d APPEND: %v \n", rf.me, rf.currentTerm, rf.logs, rf.commitIndex, *args)
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	} else {
		rf.state = Follower
		rf.currentTerm = args.Term
	}
	rf.mu.Unlock()

	// take it as heartbeat true at this moment
	reply.Term = args.Term
	rf.heatbeatSignalCh <- true

	rf.mu.Lock()
	if logSize := len(rf.logs); args.PrevLogIndex == 0 || (logSize >= args.PrevLogIndex && args.PrevLogTerm == rf.logs[args.PrevLogIndex-1].Term) {
		reply.Success = true
		indexOffset := args.PrevLogIndex + 1 - 1
		for i := 0; i < len(args.Entries); i++ {
			if i+indexOffset < logSize {
				rf.logs[i+indexOffset] = args.Entries[i]
			} else {
				rf.logs = append(rf.logs, args.Entries[i])
			}
		}
		if args.LeaderCommit >= rf.commitIndex {
			rf.commitIndex = Min(args.LeaderCommit, len(rf.logs))
		} else {
			rf.commitIndex = args.LeaderCommit
			rf.lastApplied = args.LeaderCommit
		}
	} else {
		reply.Success = false
	}
	rf.mu.Unlock()
	go rf.applyChangesToLatestCommit()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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
	isLeader = (rf.state == Leader)
	if isLeader {
		rf.logs = append(rf.logs, LogEntry{rf.currentTerm, command})
		index = len(rf.logs)
		term = rf.currentTerm
		// fmt.Printf("leader %d cmd %v Term: %d NewIndex: %d logSize: %d\n", rf.me, command, rf.currentTerm, index, len(rf.logs))
	}
	rf.mu.Unlock()

	return index, term, isLeader
}

// FollowerCheckPeriodicHeartbeat :
// A constantly running thread checking if server is receiving heartbeats
// ignore heatbeat timeout if server is in state of Leader or Candidiate
func (rf *Raft) FollowerCheckPeriodicHeartbeat() {
	for {
		timeout := FollowerHeartbeatTimeoutMilli + FollowerHeartbeatTimeoutMilli/4 -
			rand.Intn(FollowerHeartbeatTimeoutMilli/2)
		select {
		case <-rf.heatbeatSignalCh:
		case <-time.After(time.Duration(timeout) * time.Millisecond):
			// This does not require strict locking since
			// 1) don't lock around channels (might be blocking)
			// 2) Follower timeout does not need to be that precise, it is good to timeout it in next loop if server become Follower just after the judgement(we don't want elections to happen at the same time anyway)
			rf.mu.Lock()
			if rf.state == Follower {
				rf.mu.Unlock()
				rf.startNewElectionSignalCh <- true
			} else {
				rf.mu.Unlock()
			}
		}
	}

}

// FollowerStartNewRouldOfElection :
// A follower starting a new round of election based on electionSignal Channel
// If we lock the whole method with rf.mu, it will block new AppendEntries and RequestVote
func (rf *Raft) FollowerStartNewRouldOfElection() {
	for {
		<-rf.startNewElectionSignalCh
		// fmt.Printf("## Candidate %d with term %d start a new round of election wiht logs: %v\n", rf.me, rf.currentTerm, rf.logs)
		var replyArr = make([]RequestVoteReply, len(rf.peers))
		var requestVoteArgs RequestVoteArgs
		rf.mu.Lock()
		rf.currentTerm++
		rf.state = Candidate
		rf.votedFor = rf.me
		if logSize := len(rf.logs); logSize > 0 {
			requestVoteArgs = RequestVoteArgs{Term: rf.currentTerm, CandidateID: rf.me, LastLogIndex: logSize, LastLogTerm: rf.logs[logSize-1].Term}
		} else {
			requestVoteArgs = RequestVoteArgs{Term: rf.currentTerm, CandidateID: rf.me, LastLogIndex: 0, LastLogTerm: 0}
		}
		rf.mu.Unlock()
		for peerID := 0; peerID < len(rf.peers); peerID++ {
			if peerID == rf.me {
				continue
			}
			go rf.sendRequestVote(peerID, &requestVoteArgs, &replyArr[peerID])
		}
		randTimeoutMilli := ElectionTimeoutMilli + ElectionTimeoutMilli/4 - rand.Intn(ElectionTimeoutMilli/2)

		timeout := time.After(time.Duration(randTimeoutMilli) * time.Millisecond)
		tick := time.Tick(time.Duration(LeaderHeartbeatIntervalMilli) * time.Millisecond)

		// tick is for periodic check of votes, since normally we don't need to wait that long
		electionInterrupted := false
		voteActuired := 1
		for !electionInterrupted {
			select {
			case <-timeout:
				electionInterrupted = true
			case <-tick:
				voteActuired = 1
				rf.mu.Lock()
				for peerID := 0; peerID < len(rf.peers); peerID++ {
					if peerID == rf.me {
						continue
					}
					if replyArr[peerID].Term > rf.currentTerm {
						rf.state = Follower
						rf.currentTerm = replyArr[peerID].Term
						rf.votedFor = -1
					} else if replyArr[peerID].VoteGranted {
						voteActuired++
					}
				}
				electionInterrupted = (voteActuired*2 > len(rf.peers)) || rf.state == Follower
				rf.mu.Unlock()
			}
		}
		rf.mu.Lock()
		if rf.state == Follower {
			rf.mu.Unlock()
			continue
		}
		if voteActuired*2 > len(rf.peers) {
			rf.state = Leader
			// unlock before sending to channel, could use defer here, but not that familiar for now
			rf.mu.Unlock()
			rf.winElectionSignalCh <- true
		} else {
			rf.votedFor = -1
			rf.mu.Unlock()
			rf.startNewElectionSignalCh <- true
		}
	}
}

// LeaderPeriodicHeartbeat :
func (rf *Raft) LeaderPeriodicHeartbeat() {
	for {
		<-rf.winElectionSignalCh
		reqArr := make([]AppendEntryArgs, len(rf.peers))
		replyArr := make([]AppendEntryReply, len(rf.peers))
		var logSizeForHeartbeat int

		rf.mu.Lock()
		logSizeForHeartbeat = len(rf.logs)
		for peerID := 0; peerID < len(rf.peers); peerID++ {
			prevIndex := Min(rf.nextIndex[peerID]-1, len(rf.logs))
			prevTerm := 0
			if prevIndex >= 1 {
				prevTerm = rf.logs[prevIndex-1].Term
			}
			reqArr[peerID] = AppendEntryArgs{
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  prevTerm,
				Entries:      rf.logs[prevIndex-1+1:],
				LeaderCommit: rf.commitIndex}
		}
		rf.mu.Unlock()
		for peerID := 0; peerID < len(rf.peers); peerID++ {
			if peerID == rf.me {
				continue
			}
			go rf.sendAppendEntries(peerID, &reqArr[peerID], &replyArr[peerID])
		}

		time.Sleep(LeaderHeartbeatIntervalMilli * time.Millisecond)

		rf.mu.Lock()
		for peerID := 0; peerID < len(rf.peers); peerID++ {
			if peerID == rf.me {
				continue
			}
			if replyArr[peerID].Term > rf.currentTerm {
				rf.state = Follower
				rf.currentTerm = replyArr[peerID].Term
				break
			}
		}
		// if find larger term, turn into follower
		if rf.state == Follower {
			rf.mu.Unlock()
			continue
		}

		// updating nextIndex
		appendSuccessCount := 1
		for peerID := 0; peerID < len(rf.peers); peerID++ {
			if peerID == rf.me {
				continue
			}
			if !replyArr[peerID].Success {
				rf.matchIndex[peerID] = 0
				rf.nextIndex[peerID] = Max(rf.nextIndex[peerID]/2, 1)
			} else {
				appendSuccessCount++
				rf.matchIndex[peerID] = Max(rf.nextIndex[peerID]-1, 0)
				rf.nextIndex[peerID] = rf.nextIndex[peerID] + len(reqArr[peerID].Entries)
			}
		}
		if logSizeForHeartbeat > 0 && rf.logs[logSizeForHeartbeat-1].Term == rf.currentTerm && (2*appendSuccessCount) > len(rf.peers) {
			// updating leader commit index, two requirements:
			// 1) majority appending success
			// 2) heartbeat and verifying needs to be in the same term, i.e. it should not update old term entries
			rf.commitIndex = logSizeForHeartbeat
		}
		rf.mu.Unlock()
		go rf.applyChangesToLatestCommit()

		rf.winElectionSignalCh <- true
	}
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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

	// Your initialization code here (2A, 2B, 2C).
	//2A
	rf.currentTerm = 0
	rf.state = Follower
	rf.heatbeatSignalCh = make(chan bool, 3)
	rf.startNewElectionSignalCh = make(chan bool, 3)
	rf.winElectionSignalCh = make(chan bool, 3)

	//2B
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.votedFor = -1
	rf.nextIndex = make([]int, len(rf.peers))
	for i, _ := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh

	go rf.FollowerCheckPeriodicHeartbeat()
	go rf.FollowerStartNewRouldOfElection()
	go rf.LeaderPeriodicHeartbeat()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
