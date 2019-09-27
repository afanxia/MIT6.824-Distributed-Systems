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
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "labgob"

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
	Command interface{} // command for state machine
	Term    int         // term when entry was received by leader
}

// server status
type Status int

const (
	Follower Status = iota
	Candidate
	Leader
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int         // latest term server has seen
	votedFor    int         // candidateId that received vote in current term
	log         []*LogEntry // log entries

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// Volatile state on leaders
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server

	electTimer     *time.Timer // elect timer
	heartBeatTimer *time.Timer // heartBeat timer
	status         Status      // current status
	numVoted       int         // number of get voted in currentTerm
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.status == Leader
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
	Term        int // candidate's term
	CandidateId int // candidate requesting vote
	// LastLogIndex 	int 	// index of candidate's last log entry
	// LastLogTerm	 	int 	// term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Println(time.Now(), "RequestVote -- raft ", rf.me, " received request vote from candidate ", args.CandidateId)

	rf.resetElectTimer()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	fmt.Println("RequestVote -- args.Term = ", args.Term, ", rf.currentTerm = ", rf.currentTerm)
	fmt.Println("RequestVote -- rf.votedFor = ", rf.votedFor, ", args.CandidateId = ", args.CandidateId)
	if args.Term < rf.currentTerm {
		fmt.Println(time.Now(), "RequestVote -- raft ", rf.me, " will not vote since candidate's term is too small")
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = Follower
	}
	if rf.votedFor == 0 || rf.votedFor == args.CandidateId {
		if args.Term >= rf.currentTerm {
			reply.Term = args.Term
			reply.VoteGranted = true
			fmt.Println(time.Now(), "RequestVote -- raft ", rf.me, " VOTED for candidate ", args.CandidateId)
			return
		}
	}
	return
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
// AppendEntries RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	Term     int // leader's term
	LeaderId int // so follower can redirect clients
	// PrevLogIndex 	int 		// index of log entry immediately preceding new ones
	// PrevLogTerm	 	int 		// term of preLogIndex entry
	// Entries 		[]*LogEntry // log entries to store
	// LeaderCommit 	int 		// leader's commitIndex
}

//
// AppendEntries RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Println(time.Now(), "AppendEntries -- raft ", rf.me, " received append entries from Leader ", args.LeaderId)
	fmt.Println("AppendEntries -- args.Term = ", args.Term, ", rf.currentTerm = ", rf.currentTerm)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = Follower
	}

	rf.resetElectTimer()
	reply.Term = rf.currentTerm
	reply.Success = true
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
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

// resetHeartBeatTimer will reset heartBeatTimer with a time interval of 300ms.
func (rf *Raft) resetHeartBeatTimer() {
	rf.heartBeatTimer.Reset(300 * time.Millisecond)
}

// resetElectTimer will reset electTimer with a random duration in time interval of 500~800ms.
func (rf *Raft) resetElectTimer() {
	rf.electTimer.Reset(time.Duration(500+100*rand.Intn(30)) * time.Millisecond)
}

// watch will watch the AppendEntries/RequestVote RPC on this Raft server
func (rf *Raft) watch() {
	fmt.Println(time.Now(), "watching raft ", rf.me)
	for {
		select {
		case <-rf.electTimer.C:
			if _, isLeader := rf.GetState(); isLeader {
				continue
			}
			fmt.Println(time.Now(), "raft ", rf.me, " is timeout for electTimer")

			// elect timeout, becomes Candidate and sends requestVote
			rf.mu.Lock()
			rf.currentTerm++
			rf.numVoted++
			rf.status = Candidate
			rf.numVoted = 0
			rf.mu.Unlock()
			rf.resetElectTimer()

			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				go func(i int) {
					var (
						// term 		int
						// voteGranted bool
						requestVoteReply *RequestVoteReply
					)
					requestVoteReply = new(RequestVoteReply)
					fmt.Println(time.Now(), "raft ", rf.me, " is sending request vote to raft ", i)
					req := rf.sendRequestVote(i, &RequestVoteArgs{
						Term:        rf.currentTerm,
						CandidateId: rf.me,
						// LastLogIndex: 0,
						// LastLogTerm:  0,
					}, requestVoteReply)
					fmt.Println(time.Now(), " AA  req = ", req, ", rf.currentTerm = ", rf.currentTerm, ", term = ", requestVoteReply.Term, ", voteGranted = ", requestVoteReply.VoteGranted)
					rf.mu.Lock()
					if req {
						if requestVoteReply.Term > rf.currentTerm {
							rf.currentTerm = requestVoteReply.Term
							rf.status = Follower
						}
						if rf.currentTerm == requestVoteReply.Term && requestVoteReply.VoteGranted {
							rf.numVoted++
							fmt.Println(time.Now(), "raft ", rf.me, " received request vote from raft ", i, " , total numVoted ", rf.numVoted)
						}
					}
					rf.mu.Unlock()
				}(i)
			}

			// goroutine to watch requestVoteReplies
			// wait for 1s at most
			go func() {
				var (
					i        int
					numVoted int
				)
				fmt.Println(time.Now(), " --- watching raft ", rf.me, "'s total numVote")
				i = 0
				for {
					fmt.Println(time.Now(), " --- watching ...    rf.numVoted = ", rf.numVoted, ", num of rf.peers = ", len(rf.peers))
					rf.mu.Lock()
					numVoted = rf.numVoted
					rf.mu.Unlock()
					if numVoted >= (len(rf.peers)-1)/2 {
						rf.status = Leader
						fmt.Println(time.Now(), " --- watching raft ", rf.me, " is LEADER now")
						rf.resetHeartBeatTimer()
						for i := range rf.peers {
							if i == rf.me {
								continue
							}
							go func(i int) {
								var (
									// term 		int
									// success 	bool
									appendEntriesReply *AppendEntriesReply
								)
								appendEntriesReply = new(AppendEntriesReply)
								fmt.Println(time.Now(), "raft ", rf.me, " is sending append entries to raft ", i)
								rf.sendAppendEntries(i, &AppendEntriesArgs{
									Term:     rf.currentTerm,
									LeaderId: rf.me,
									// PrevLogIndex: 0,
									// PrevLogTerm:  0,
									// Entries:      nil,
									// LeaderCommit: 0,
								}, appendEntriesReply)
							}(i)
						}

						return
					}
					time.Sleep(100 * time.Millisecond)
					i++
					if i > 10 {
						fmt.Println(time.Now(), " --- watching raft ", rf.me, " byebye~, vote failed for currentTerm")
						return
					}
				}
			}()

		case <-rf.heartBeatTimer.C:
			if _, isLeader := rf.GetState(); !isLeader {
				continue
			}
			fmt.Println(time.Now(), "raft ", rf.me, " is timeout for heartBeatTimer")
			// heartBeat timeout, send AppendEntries(command and heartBeat)
			rf.resetHeartBeatTimer()

			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				go func(i int) {
					var (
						// term 		int
						// success 	bool
						appendEntriesReply *AppendEntriesReply
					)
					appendEntriesReply = new(AppendEntriesReply)
					fmt.Println(time.Now(), "raft ", rf.me, " is sending append entries to raft ", i)
					rf.sendAppendEntries(i, &AppendEntriesArgs{
						Term:     rf.currentTerm,
						LeaderId: rf.me,
						// PrevLogIndex: 0,
						// PrevLogTerm:  0,
						// Entries:      nil,
						// LeaderCommit: 0,
					}, appendEntriesReply)
				}(i)
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.status = Follower

	fmt.Println("starting raft ", me)
	rf.electTimer = time.NewTimer(time.Duration(500+100*rand.Intn(30)) * time.Millisecond)
	rf.heartBeatTimer = time.NewTimer(300 * time.Millisecond)

	go rf.watch()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
