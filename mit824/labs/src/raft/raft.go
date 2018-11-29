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
	"fmt"
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
const LeaderHeartbeatIntervalMilli = 20

// FollowerHeartbeatTimeoutMilli : Define Follower heartbeat timeout here.
const FollowerHeartbeatTimeoutMilli = 500

// ElectionTimeoutMilli : Define Election timeout of each term here.
const ElectionTimeoutMilli = 100

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
	votedFor         int
	lastHearbeatTime time.Time
	heartbeatCh      chan bool
	electionSignalCh chan bool
	leaderSignalCh   chan bool

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
	Term        int
	CandidateID int
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
	// Your code here (2A, 2B).
	if rf.currentTerm < args.Term {
		// leader or candidate to follower, state transition happens here
		rf.currentTerm = args.Term
		rf.workerState = Follower
		rf.votedFor = args.CandidateID

		reply.Term = args.Term
		reply.VoteGranted = true
		rf.heartbeatCh <- true
	} else if rf.currentTerm > args.Term {
		// not on the same page, declines always, no state or term change
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else {
		reply.Term = rf.currentTerm
		// on the same page, only a candidate can vote for others and become a Follower.
		// leader and follower kind of "freezed" on this term. (One worker can only vote for one in a term)
		if rf.workerState == Candidate {
			// in case RPC is used for its own requestVote, do not follow itself
			rf.votedFor = args.CandidateID
			if args.CandidateID != rf.me {
				rf.workerState = Follower
			}

			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
		rf.heartbeatCh <- true
	}

	// else {
	// 	// if requestor term equals or smaller than currentTerm, consider
	// 	// leader: should not approve vote, should not change state
	// 	// candidate: should not approve vote, should not change state
	// 	// follower: voted for another candidate, should not change state
	// 	reply.term = rf.currentTerm
	// 	if args.candidateId == rf.me {
	// 		reply.voteGranted = true
	// 	} else {
	// 		reply.voteGranted = false
	// 	}
	// }
}

type AppendEntriesArgs struct {
	Term     int
	LeaderID int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries can only be sent from candidate(??? still confusing here) or leader to make sure it has 'leadership' to you
// Each worker must only vote for one leader at a time, it means we need to make a difference between currentTerm > other.term and currentTerm = other.term
// for currentTerm > other.term, it declines the request anyway, since it is outdated
// for currentTerm == other.term, they are on the same page, we should discuss the state
// keep in mind, if a worker is candidate, it already votes for itself
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.currentTerm < args.Term {
		// currentTerm is smaller, no matter what state you are in, become Follower, update term
		// Approve appendEntry
		rf.workerState = Follower
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderID

		reply.Term = args.Term
		reply.Success = true
		rf.heartbeatCh <- true
	} else if rf.currentTerm > args.Term {
		// Rejects "leadership" claim, just ignore outdated workers
		reply.Term = rf.currentTerm
		reply.Success = false
	} else {
		// if they are on the same page, it is almost the same as <, but let's keep it here for a better explanation
		// i am thinking about two leaders and followers with different leaders, it seems that we don't need to consider this for now
		// let's assume it would only be Candidate and Follower
		if args.LeaderID != rf.me {
			rf.workerState = Follower
			rf.votedFor = args.LeaderID
		}

		reply.Term = args.Term
		reply.Success = true
		rf.heartbeatCh <- true
	}
}

// LaunchPeriodicHeartbeatCheck :
func (rf *Raft) LaunchPeriodicHeartbeatCheck(heartbeatCh chan bool, electionSignalCh chan bool) {
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

// LaunchStartNewRoundElectionListen :
func (rf *Raft) LaunchStartNewRoundElectionListen(electionSignalCh chan bool, leaderSignalCh chan bool) {
	for {
		<-electionSignalCh
		// fmt.Printf("%d start new round of election with term %d \n", rf.me, rf.currentTerm)
		rf.mu.Lock()
		rf.currentTerm++
		rf.workerState = Candidate
		rf.mu.Unlock()

		replyArr := make([]RequestVoteReply, len(rf.peers))
		requestArgs := RequestVoteArgs{rf.currentTerm, rf.me}
		for peerID := 0; peerID < len(rf.peers); peerID++ {
			go rf.sendRequestVote(peerID, &requestArgs, &replyArr[peerID])
		}
		randTimeoutMilli := ElectionTimeoutMilli + ElectionTimeoutMilli/4 -
			rand.Intn(ElectionTimeoutMilli/2)
		time.Sleep(time.Duration(randTimeoutMilli) * time.Millisecond)
		// timeout := time.After(time.Duration(randTimeoutMilli) * time.Millisecond)
		// // here tick can be set as random values, but i assume the RPC reply time will be similar to hearbeat time
		// tick := time.Tick(time.Duration(FollowerHeartbeatTimeoutMilli) * time.Millisecond)

		voteAcquired := 0
		// electionInterrupted := false
		// for !electionInterrupted {
		// 	select {
		// 	case <-timeout:
		// 		electionInterrupted = true
		// 	case <-tick:
		// 		for peerID := 0; peerID < len(rf.peers); peerID++ {
		// 			if replyArr[peerID].Term > rf.currentTerm {
		// 				rf.workerState = Follower
		// 				rf.votedFor = peerID
		// 				break
		// 			} else if replyArr[peerID].VoteGranted {
		// 				voteAcquired++
		// 			}
		// 		}
		// 		electionInterrupted = (voteAcquired*2 > len(rf.peers))
		// 	}
		// }
		for peerID := 0; peerID < len(rf.peers); peerID++ {
			if replyArr[peerID].Term > rf.currentTerm {
				rf.workerState = Follower
				rf.votedFor = peerID
				break
			} else if replyArr[peerID].VoteGranted {
				voteAcquired++
			}
		}

		// fmt.Printf("%d get %d votes with state: %d\n", rf.me, voteAcquired, rf.workerState)

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

// LaunchLeaderHeartbeatListen :
func (rf *Raft) LaunchLeaderHeartbeatListen(leaderSignalCh chan bool) {
	for {
		<-leaderSignalCh
		// fmt.Printf("LEADER now: %d state: %d\n", rf.me, rf.workerState)

		replyArr := make([]AppendEntriesReply, len(rf.peers))
		for peerID := 0; peerID < len(rf.peers); peerID++ {
			appendEntriesArgs := AppendEntriesArgs{rf.currentTerm, rf.me}
			go rf.sendAppendEntries(peerID, &appendEntriesArgs, &replyArr[peerID])
		}

		time.Sleep(LeaderHeartbeatIntervalMilli * time.Millisecond)

		for peerID := 0; peerID < len(rf.peers); peerID++ {
			if replyArr[peerID].Term > rf.currentTerm {
				rf.workerState = Follower
				break
			}
		}
		// fmt.Printf("LEADER end state: %d\n", rf.workerState)
		if rf.workerState == Leader {
			leaderSignalCh <- true
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
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
	term := 0
	isLeader := false
	fmt.Println("Server Starting")
	go rf.LaunchPeriodicHeartbeatCheck(rf.heartbeatCh, rf.electionSignalCh)
	go rf.LaunchStartNewRoundElectionListen(rf.electionSignalCh, rf.leaderSignalCh)
	go rf.LaunchLeaderHeartbeatListen(rf.leaderSignalCh)

	rf.electionSignalCh <- true

	// Your code here (2B).

	return index, term, isLeader
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
	rf.votedFor = me
	rf.heartbeatCh = make(chan bool, 3)
	rf.electionSignalCh = make(chan bool, 3)
	rf.leaderSignalCh = make(chan bool, 3)

	go rf.LaunchPeriodicHeartbeatCheck(rf.heartbeatCh, rf.electionSignalCh)
	go rf.LaunchStartNewRoundElectionListen(rf.electionSignalCh, rf.leaderSignalCh)
	go rf.LaunchLeaderHeartbeatListen(rf.leaderSignalCh)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
