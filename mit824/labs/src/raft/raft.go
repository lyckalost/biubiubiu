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
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

// ApplyMsg :
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

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
const FollowerHeartbeatTimeoutMilli = 300

// ElectionTimeoutMilli : Define Election timeout of each term here.
const ElectionTimeoutMilli = 1000

// LogEntry :
type LogEntry struct {
	Term    int
	Command interface{}
}

// AtomicLogSlice :
type AtomicLogSlice struct {
	mu   sync.Mutex
	logs []LogEntry
}

func (al *AtomicLogSlice) Size() int {
	var res int
	al.mu.Lock()
	res = len(al.logs)
	al.mu.Unlock()
	return res
}

func (al *AtomicLogSlice) Get(i int) LogEntry {
	var res LogEntry
	al.mu.Lock()
	res = al.logs[i]
	al.mu.Unlock()
	return res
}

func (al *AtomicLogSlice) Set(i int, entry LogEntry) {
	al.mu.Lock()
	al.logs[i] = entry
	al.mu.Unlock()
}

func (al *AtomicLogSlice) Append(entry LogEntry) int {
	var res int
	al.mu.Lock()
	al.logs = append(al.logs, entry)
	res = len(al.logs) - 1
	al.mu.Unlock()
	return res
}

func (al *AtomicLogSlice) Slice(start int, end int) []LogEntry {
	var res []LogEntry
	al.mu.Lock()
	for i := start; i < end && i < len(al.logs); i++ {
		res = append(res, al.logs[i])
	}
	al.mu.Unlock()
	return res
}

func (al *AtomicLogSlice) GetAll() []LogEntry {
	return al.logs
}

// Raft :
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	//2A
	currentTerm      int
	workerState      WorkerState
	heartbeatCh      chan bool
	electionSignalCh chan bool
	leaderSignalCh   chan bool

	//2B
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	logs        []LogEntry
	applyCh     chan ApplyMsg
	atomLogs    AtomicLogSlice
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// GetState :
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	// Your code here (2A).
	term = rf.currentTerm
	isleader = (rf.workerState == Leader)

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
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply :
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// RequestVote :
// example RequestVote RPC handler.
// RequestVote can only be sent from candidate
// Each worker must only vote for one leader at a time, it means we need to make a difference between currentTerm > other.term and currentTerm = other.term
// for currentTerm > other.term, it declines the request anyway, since it is outdated
// for currentTerm == other.term, they are on the same page, we should discuss the state
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	// by default not vote
	reply.Term = Max(rf.currentTerm, args.Term)
	reply.VoteGranted = false

	// Your code here (2A, 2B).
	if rf.currentTerm < args.Term {
		// leader or candidate to follower, state transition happens here
		// always update state before modifying terms if race conditions are not well handled
		rf.workerState = Follower
		rf.currentTerm = args.Term

		// don't think this can ensure log entry coverage
		if args.LastLogIndex >= rf.commitIndex && (rf.commitIndex == 0 || args.LastLogTerm >= rf.atomLogs.Get(rf.commitIndex-1).Term) {
			// if rf.commitIndex != 0 {
			// 	fmt.Printf("Server %d giving vote to Candidate %d with LastLogIndex: %d LastLogTerm: %d vs server commitIndex: %d logSize: %d term: %d\n ", rf.me, args.CandidateID, args.LastLogIndex, args.LastLogTerm, rf.commitIndex, len(rf.logs), rf.logs[rf.commitIndex-1].Term)
			// }
			reply.VoteGranted = true
		}
	} else if rf.currentTerm > args.Term {
		// not on the same page, declines always, no state or term change
	} else {
		// on the same page, only a candidate can vote for others and become a Follower.
		// leader and follower kind of "freezed" on this term. (One worker can only vote for one in a term)
		if rf.workerState == Candidate {
			// in case RPC is used for its own requestVote, do not follow itself
			if args.CandidateID != rf.me {
				rf.workerState = Follower
			}

			if args.LastLogIndex >= rf.commitIndex && (rf.commitIndex == 0 || args.LastLogTerm >= rf.atomLogs.Get(rf.commitIndex-1).Term) {
				// if rf.commitIndex != 0 {
				// 	fmt.Printf("Server %d giving vote to Candidate %d with LastLogIndex: %d LastLogTerm: %d vs server commitIndex: %d logSize: %d term: %d\n ", rf.me, args.CandidateID, args.LastLogIndex, args.LastLogTerm, rf.commitIndex, len(rf.logs), rf.logs[rf.commitIndex-1].Term)
				// }
				reply.VoteGranted = true
			}
		}
	}
	rf.mu.Unlock()
	rf.heartbeatCh <- true
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func Min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func Max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func (rf *Raft) applyChangesToLatestCommit() {

	if rf.commitIndex > rf.lastApplied {
		// fmt.Printf("Server %d applying from %d to %d %v\n", rf.me, rf.lastApplied+1, rf.commitIndex, rf.atomLogs.GetAll())

		for i := rf.lastApplied + 1 - 1; i <= rf.commitIndex-1; i++ {
			// i + 1 for mapping index in array to logical index
			rf.applyCh <- ApplyMsg{true, rf.atomLogs.Get(i).Command, i + 1}
		}
		rf.lastApplied = rf.commitIndex
	}
}

// SynEntriesWithLeader :
// actually this method performs two operations :
// 1) sync with learder about the entries
// 2) apply entries to latest leader commit
func (rf *Raft) SynEntriesWithLeader(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	logSizeBeforeSync := rf.atomLogs.Size()
	offset := args.PrevLogIndex - 1 + 1
	for i, entry := range args.Entries {
		if i+offset < logSizeBeforeSync {
			rf.atomLogs.Set(i+offset, entry)
		} else {
			rf.atomLogs.Append(entry)
		}
	}

	// fmt.Printf("SYNC server: %d from leader %d prevIndex: %d logs: %v args: %v\n", rf.me, args.LeaderID, args.PrevLogIndex, rf.logs, args.Entries)
	// syncing with learder about commits
	if args.LeaderCommit >= rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, rf.atomLogs.Size())
	} else {
		// this else seems wired to me, imagine this
		// server 0, 1, 2
		// 0 is the leader, 1, 2 follower
		// 0, 1 running all good at term 1 with index up to 6
		// 2 network failed at after index2, and started to launch election and update terms
		// 2 network resumed at term4, will win the election (with term4)
		// 2 largest index is 1 will erase all 0, 1 commits from 2 - 6
		// Nooooooooo! 2 can not win election in case above.
		rf.commitIndex = args.LeaderCommit
		rf.lastApplied = args.LeaderCommit
	}
	go rf.applyChangesToLatestCommit()
}

// AppendEntries can only be sent from candidate(??? still confusing here) or leader to make sure it has 'leadership' to you
// Each worker must only vote for one leader at a time, it means we need to make a difference between currentTerm > other.term and currentTerm = other.term
// for currentTerm > other.term, it declines the request anyway, since it is outdated
// for currentTerm == other.term, they are on the same page, we should discuss the state
// keep in mind, if a worker is candidate, it already votes for itself
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if rf.me == args.LeaderID {
		reply.Success = true
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	if rf.currentTerm > args.Term {
		// Rejects "leadership" claim, just ignore outdated workers
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	if rf.currentTerm < args.Term {
		// currentTerm is smaller, no matter what state you are in, become Follower, update term
		// Approve appendEntry
		rf.workerState = Follower
		rf.currentTerm = args.Term
	} else {
		// if they are on the same page, it is almost the same as <, but let's keep it here for a better explanation
		// i am thinking about two leaders and followers with different leaders, it seems that we don't need to consider this for now
		// let's assume it would only be Candidate and Follower
		if args.LeaderID != rf.me {
			rf.workerState = Follower
		}
	}
	rf.mu.Unlock()

	// here we consider heartbeat as success, overrall success depends on log entry checking
	reply.Term = args.Term
	rf.heartbeatCh <- true

	if args.PrevLogIndex == 0 || (rf.atomLogs.Size() >= args.PrevLogIndex && rf.atomLogs.Get(args.PrevLogIndex-1).Term == args.PrevLogTerm) {
		// we need to put it even with empty entries to append also here since SynEntriesWithLeader perform both sync and apply operation
		rf.SynEntriesWithLeader(args, reply)
		reply.Success = true
	} else {
		reply.Success = false
	}
	// fmt.Printf("APPEDN--- Server: %d commitIndex: %d LeaderCommig: %d logSize: %d term:  %d prevIndex: %d prevTerm: %d argsSize : %d ### %v returning %v \n", rf.me, rf.commitIndex, args.LeaderCommit, rf.atomLogs.Size(), args.Term, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.Entries, reply.Success)
}

// FollowerPeriodicHeartbeatCheck : This is for the follower
func (rf *Raft) FollowerPeriodicHeartbeatCheck(heartbeatCh chan bool, electionSignalCh chan bool) {
	for {
		timeout := FollowerHeartbeatTimeoutMilli + FollowerHeartbeatTimeoutMilli/4 -
			rand.Intn(FollowerHeartbeatTimeoutMilli/2)
		select {
		case <-heartbeatCh:
			// fmt.Printf("%d Heartbeat at time: "+time.Now().String()+"\n", rf.me)
		case <-time.After(time.Duration(timeout) * time.Millisecond):
			// fmt.Printf("%d Heartbeat failed at time: "+time.Now().String()+"\n", rf.me)
			// if not Follower, just ignore
			if rf.workerState != Follower {
				continue
			}
			// if Follower, signal to start a new round of election
			electionSignalCh <- true
		}
	}
}

// FollowerListenOnNewElectionSingal :
func (rf *Raft) FollowerListenOnNewElectionSingal(electionSignalCh chan bool, leaderSignalCh chan bool) {
	for {
		<-electionSignalCh
		// fmt.Printf("%d start new round of election with term %d \n", rf.me, rf.currentTerm)
		// 1) preparing server state for requesting votes
		rf.mu.Lock()
		rf.currentTerm++
		rf.workerState = Candidate
		rf.mu.Unlock()

		// 2) sending request votes to others
		replyArr := make([]RequestVoteReply, len(rf.peers))
		var requestArgs RequestVoteArgs
		if logSize := rf.atomLogs.Size(); logSize > 0 {
			requestArgs = RequestVoteArgs{rf.currentTerm, rf.me, logSize, rf.atomLogs.Get(logSize - 1).Term}
		} else {
			requestArgs = RequestVoteArgs{rf.currentTerm, rf.me, 0, rf.currentTerm}
		}
		for peerID := 0; peerID < len(rf.peers); peerID++ {
			go rf.sendRequestVote(peerID, &requestArgs, &replyArr[peerID])
		}
		randTimeoutMilli := ElectionTimeoutMilli + ElectionTimeoutMilli/4 -
			rand.Intn(ElectionTimeoutMilli/2)
		timeout := time.After(time.Duration(randTimeoutMilli) * time.Millisecond)
		// here tick can be set as random values, but i assume the RPC reply time will be similar to hearbeat time
		tick := time.Tick(time.Duration(LeaderHeartbeatIntervalMilli) * time.Millisecond)

		voteAcquired := 0
		electionInterrupted := false
		for !electionInterrupted {
			// time out is for election time out
			// tick is for periodic check in case of stopping waiting ahead of time a) got a higher term b) got enough votes
			select {
			case <-timeout:
				electionInterrupted = true
			case <-tick:
				voteAcquired = 0
				for peerID := 0; peerID < len(rf.peers); peerID++ {
					if replyArr[peerID].Term > rf.currentTerm {
						rf.workerState = Follower
						electionInterrupted = true
					} else if replyArr[peerID].VoteGranted {
						voteAcquired++
					}
				}
				electionInterrupted = (voteAcquired*2 > len(rf.peers))
			}
		}

		// fmt.Printf("%d get %d votes with state: %d\n", rf.me, voteAcquired, rf.workerState)

		// 3) gathering votes and change states
		if rf.workerState == Follower {
			continue
		}

		if voteAcquired*2 > len(rf.peers) {
			rf.workerState = Leader
			leaderSignalCh <- true
		} else {
			electionSignalCh <- true
		}

	}
}

// LeaderPeriodicHeartbeat : the thread for leader to append entries/send heartbeats
func (rf *Raft) LeaderPeriodicHeartbeat(leaderSignalCh chan bool) {
	for {
		<-leaderSignalCh
		logsSizeBeforeHearbeating := rf.atomLogs.Size()
		termBeforeHearbeating := rf.currentTerm

		replyArr := make([]AppendEntriesReply, len(rf.peers))
		reqArr := make([]AppendEntriesArgs, len(rf.peers))
		for peerID := 0; peerID < len(rf.peers); peerID++ {
			var prevIndex, prevTerm int
			var entriesForAppend []LogEntry
			if rf.nextIndex[peerID] > 1 {
				prevIndex = rf.nextIndex[peerID] - 1
				prevTerm = rf.atomLogs.Get(prevIndex - 1).Term
				// prevIndex - 1 for mapping logical index to log array index, +1 for start of nextIndex
				entriesForAppend = rf.atomLogs.Slice(prevIndex-1+1, logsSizeBeforeHearbeating)
			} else {
				entriesForAppend = rf.atomLogs.Slice(0, logsSizeBeforeHearbeating)
			}

			reqArr[peerID] = AppendEntriesArgs{termBeforeHearbeating, rf.me, prevIndex, prevTerm, entriesForAppend, rf.commitIndex}
			go rf.sendAppendEntries(peerID, &reqArr[peerID], &replyArr[peerID])
		}

		time.Sleep(LeaderHeartbeatIntervalMilli * time.Millisecond)

		// update nextIndex for each follower according to append status
		for peerID := 0; peerID < len(rf.peers); peerID++ {
			// fmt.Printf("%d term %d vs Leader %d term %d \n", peerID, replyArr[peerID].Term, rf.me, termBeforeHearbeating)
			if replyArr[peerID].Term > termBeforeHearbeating {
				rf.workerState = Follower
			}
			if !replyArr[peerID].Success {
				rf.nextIndex[peerID] = Max(rf.nextIndex[peerID]/2, 1)
				rf.matchIndex[peerID] = Max(rf.nextIndex[peerID]-1, 0)
			} else {
				// cause sending all entries from Prev to End
				// These two are wrong examples about synchronization problem, avoid using things like rf.logs which might cause race condition
				// it could happen that, leader is taking new entry while syncing the old entries to followers, at this time the new entry will be considered as synced but acutally not
				rf.matchIndex[peerID] = rf.matchIndex[peerID] + len(reqArr[peerID].Entries)
				// if rf.matchIndex[peerID]+1 != rf.nextIndex[peerID] {
				// 	fmt.Printf("updating nextIndex for %d from %d to %d with entries: %v\n", peerID, rf.nextIndex[peerID], rf.matchIndex[peerID]+1, reqArr[peerID])
				// }
				rf.nextIndex[peerID] = rf.matchIndex[peerID] + 1
			}
		}

		// this server could not be leader if some follower returned a higher term
		if rf.workerState == Leader {
			go rf.applyChangesToLatestCommit()

			for i := rf.commitIndex + 1; i <= logsSizeBeforeHearbeating; i++ {
				successCount := 0
				for peerID := 0; peerID < len(rf.peers); peerID++ {
					if replyArr[peerID].Success {
						successCount++
					}
				}
				if 2*successCount > len(rf.peers) && rf.atomLogs.Get(i-1).Term == termBeforeHearbeating {
					// fmt.Printf("UPDATING learder %d commitIndex from %d to %d\n", rf.me, rf.commitIndex, i)
					rf.commitIndex = i
				}
			}
			// apply newly commited changes
			leaderSignalCh <- true
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// fmt.Printf("server %d sending append entry\n", rf.me)
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
	// fmt.Printf("server %d sending request vote\n", rf.me)
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
	if rf.workerState != Leader {
		return 0, 0, false
	} else {
		newLogEntry := LogEntry{rf.currentTerm, command}
		newIndex := rf.atomLogs.Append(newLogEntry) + 1

		// fmt.Printf("leader %d cmd %v Term: %d NewIndex: %d logSize: %d\n", rf.me, command, rf.currentTerm, newIndex, rf.atomLogs.Size())
		return newIndex, rf.currentTerm, true
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

	//2A
	rf.currentTerm = 0
	rf.workerState = Follower
	rf.heartbeatCh = make(chan bool, 3)
	rf.electionSignalCh = make(chan bool, 3)
	rf.leaderSignalCh = make(chan bool, 3)

	//2B
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh

	go rf.FollowerPeriodicHeartbeatCheck(rf.heartbeatCh, rf.electionSignalCh)
	go rf.FollowerListenOnNewElectionSingal(rf.electionSignalCh, rf.leaderSignalCh)
	go rf.LeaderPeriodicHeartbeat(rf.leaderSignalCh)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
