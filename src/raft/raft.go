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
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Term    int
	Command interface{}
}

const (
	Follower = iota
	Candidate
	Leader
)

const (
	HeartBeatTimeOut = 50
	ElectTimeout     = 150 // 选举
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []Entry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	timeStamp time.Time
	role      int

	voteCount int //获得的票数
	applyCh   chan ApplyMsg

	lastIncludedIndex int // the snapshot replaces all entries up through and including this index
	lastIncludedTerm  int // term of lastIncludedIndex
}

func (rf *Raft) realidx2virtualidx(realidx int) int {
	// 使用前需用锁
	return realidx + rf.lastIncludedIndex
}

func (rf *Raft) virtual2realidx(viridx int) int {
	// 使用前需用锁，统一用virtual index索引
	return viridx - rf.lastIncludedIndex
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
	isleader = (rf.role == Leader)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var votedFor int
	var currentTerm int
	var log []Entry
	if d.Decode(&votedFor) != nil ||
		d.Decode(&currentTerm) != nil || d.Decode(&log) != nil {
		DPrintf(dError, "readPersist failed\n")
	} else {
		rf.votedFor = votedFor
		rf.currentTerm = currentTerm
		rf.log = log
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock() // 锁，可能会有多个同时调用
	defer rf.mu.Unlock()
	DPrintf(dVote, "server:%v收到候选者%v票\n", rf.me, args.CandidateId)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		DPrintf(dVote, "候选者term小于当前,server %v 拒绝向 server %v投票: 候选者term:%v,当前serverTerm:%v\n", rf.me, args.CandidateId, rf.currentTerm, rf.currentTerm)
	} else {
		if rf.currentTerm < args.Term { // 说明是新一轮投票，重新竞选
			rf.votedFor = -1
			rf.role = Follower
		}
		rf.currentTerm = args.Term // 更新为能获得的最大的term,currentTerm不可能变小
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			if (args.LastLogTerm > rf.log[len(rf.log)-1].Term) || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1) {
				// 同意，投票
				reply.VoteGranted = true
				rf.votedFor = args.CandidateId
				rf.role = Follower
				rf.timeStamp = time.Now()
				DPrintf(dVote, "server %v 同意向 server %v投票,len(log)%v,lastlogterm:%v\n", rf.me, args.CandidateId, len(rf.log), rf.log[len(rf.log)-1])
			} else {
				DPrintf(dVote, "日志不够新,server %v 拒绝向server %v投票\n", rf.me, args.CandidateId)
			}
		} else {
			DPrintf(dVote, "server %v 拒绝向 server %v投票: 已投票:%v\n", rf.me, args.CandidateId, rf.votedFor)
		}
	}
	reply.Term = rf.currentTerm
	rf.persist()
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
	Xterm   int //  term in the conflicting entry (if any)
	XIndex  int // index of first entry with that term (if any)
	Xlen    int //  log length
}

func (rf *Raft) handleHeartBeat(serverTo int, argsP *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(serverTo, argsP, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader {
		return
	}
	if !reply.Success {
		if reply.Term > rf.currentTerm {
			// 转为follower
			DPrintf(dLeader, "旧leader:%v收到了%v心跳返回的新term: %v, 转化为Follower,旧term: %v\n", rf.me, serverTo, reply.Term, rf.currentTerm)
			rf.role = Follower
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.timeStamp = time.Now()
		} else {
			// 重试
			if reply.Xterm == -1 {
				//说明follower的日志缺失
				rf.nextIndex[serverTo] = reply.Xlen
				DPrintf(dLog2, "leader:%v处理日志缺失,server:%v,argsP.PrevLogIndex:%v\n", rf.me, serverTo, argsP.PrevLogIndex)
			} else {
				// 说明是日志项不匹配
				DPrintf(dLog2, "leader:%v处理日志冲突,server:%v,argsP.PrevLogIndex:%v,Xterm:%v,Xidx:%v\n", rf.me, serverTo, argsP.PrevLogIndex, reply.Xterm, reply.XIndex)
				rf.nextIndex[serverTo] = rf.FindTermFirstIdx(argsP.PrevLogTerm, argsP.PrevLogIndex)
			}
		}

	} else {
		// 一次添加多个log项
		//If successful: update nextIndex and matchIndex for follower (§5.3)
		if argsP.Entries != nil {
			rf.nextIndex[serverTo] = argsP.PrevLogIndex + len(argsP.Entries) + 1
			rf.matchIndex[serverTo] = rf.nextIndex[serverTo] - 1
			DPrintf(dLog2, "返回成功,更新server:%v的nextIndex:%v,argsP.PrevLogIndex:%v\n", serverTo, rf.nextIndex[serverTo], argsP.PrevLogIndex)
		}
	}
	rf.persist()
}

func (rf *Raft) updateLastApply() {
	// lock before use
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied += 1
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		DPrintf(dCommit, "server %v commit,commitidx:%v,lastApplied:%v,lastappliedTerm%v\n", rf.me, rf.commitIndex, rf.lastApplied, rf.log[rf.lastApplied].Term)
	}
}

func (rf *Raft) SendHeartBeats() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}
		DPrintf(dLeader, "leader %v 发送心跳Term:%v,leader log len:%v,lastlogterm:%v\n", rf.me, rf.currentTerm, len(rf.log), rf.log[len(rf.log)-1].Term)
		// FIXME: 锁
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
				LeaderCommit: rf.commitIndex,
			}
			if len(rf.log) > rf.nextIndex[i] {
				DPrintf(dCommit, "leader %v 向 server %v 添加新的Entries,leader log len:%v,follower nextidx:%v\n", rf.me, i, len(rf.log), rf.nextIndex[i])
				args.Entries = rf.log[rf.nextIndex[i]:]
			}
			go rf.handleHeartBeat(i, &args)
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(HeartBeatTimeOut) * time.Millisecond)
		/*
			If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
			set commitIndex = N (§5.3, §5.4).
		*/
		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}
		N := len(rf.log) - 1
		for N > rf.commitIndex {
			cnt := 1
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				if rf.matchIndex[i] >= N && rf.log[N].Term == rf.currentTerm {
					cnt += 1
				}
			}
			if cnt >= (len(rf.peers)+1)/2 {
				break
			}
			N -= 1
		}
		rf.commitIndex = N
		rf.updateLastApply()
		rf.mu.Unlock()
	}
}

func (rf *Raft) AppendEntriesReceive(argsP *AppendEntriesArgs, replyP *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if argsP.Term < rf.currentTerm {
		replyP.Term = rf.currentTerm
		replyP.Success = false
		return
	}
	DPrintf(dLog, "server %v 收到leader %v的心跳,serverTerm%v,len(log)%v,lastlogterm%v\n", rf.me, argsP.LeaderId, rf.currentTerm, len(rf.log), rf.log[len(rf.log)-1].Term)
	rf.timeStamp = time.Now()

	if argsP.Term > rf.currentTerm {
		rf.currentTerm = argsP.Term
		rf.role = Follower
		rf.votedFor = -1
		rf.persist()
	}
	if argsP.PrevLogIndex >= len(rf.log) {
		// 过长
		replyP.Xterm = -1
		replyP.Xlen = len(rf.log)
		replyP.Term = rf.currentTerm
		replyP.Success = false
		return
	}
	if rf.log[argsP.PrevLogIndex].Term != argsP.PrevLogTerm {
		DPrintf(dLog, "日志项不匹配server %v,leader %v,argsP.PrevLogIndex:%v,rf.log len:%v\n", rf.me, argsP.LeaderId, argsP.PrevLogIndex, len(rf.log))
		replyP.Xterm = rf.log[argsP.PrevLogIndex].Term
		replyP.Term = rf.currentTerm
		replyP.Success = false
		return
	}
	if argsP.Entries != nil {
		if argsP.PrevLogIndex+1 < len(rf.log) && rf.log[argsP.PrevLogIndex+1].Term != argsP.Entries[0].Term {
			DPrintf(dLog, "日志项冲突server:%v,leader:%v\n", rf.me, argsP.LeaderId)
			// 3. If an existing entry conflicts with a new one (same index
			// but different terms), delete the existing entry and all that
			// follow it (§5.3)
			//rf.log = rf.log[:argsP.PrevLogIndex+1]
		}
		// 4. Append any new entries not already in the log ，防止同一个log重复添加
		rf.log = append(rf.log[:argsP.PrevLogIndex+1], argsP.Entries...)
		DPrintf(dLog2, "server:%v添加日志项len: %v,lastlogterm%v", rf.me, len(rf.log), rf.log[len(rf.log)-1].Term)
		rf.persist()
	}
	if argsP.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(argsP.LeaderCommit), float64(len(rf.log)-1)))
		rf.updateLastApply()
	}
	replyP.Success = true
	replyP.Term = rf.currentTerm
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntriesReceive", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader {
		return -1, -1, false
	}
	rf.log = append(rf.log, Entry{Term: rf.currentTerm, Command: command})
	rf.persist()
	return len(rf.log) - 1, rf.currentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) Elect() {

	rf.mu.Lock()
	rf.currentTerm += 1
	rf.role = Candidate
	rf.votedFor = rf.me
	rf.timeStamp = time.Now() // 重新倒计时
	rf.voteCount = 1
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	DPrintf(dVote, "server %v发起公投,Term:%v,len(log):%v,lastlogterm %v\n", rf.me, rf.currentTerm, len(rf.log), rf.log[len(rf.log)-1].Term)
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go rf.colloctVote(args, i)
	}

}

// 收集票,收集途中，有可能变为follower
func (rf *Raft) colloctVote(args *RequestVoteArgs, serverTo int) {

	reply := RequestVoteReply{}
	if !rf.sendRequestVote(serverTo, args, &reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		// 过时的任期
		rf.currentTerm = reply.Term
		rf.role = Follower
		rf.votedFor = -1
		rf.persist()
		return
	}
	if reply.VoteGranted && rf.currentTerm == args.Term {
		// 要同一个任期内才计，有可能一个包很久才回来
		rf.voteCount += 1
	}
	DPrintf(dVote, "server %v 统计票:%d,role:%v,currentTerm%v,argTerm:%v\n", rf.me, rf.voteCount, rf.role, rf.currentTerm, args.Term)
	if rf.role == Candidate && rf.voteCount == len(rf.peers)/2+1 {
		DPrintf(dVote, "Server %v 成为Leader,loglen%v,lastlogterm%v", rf.me, len(rf.log), rf.log[len(rf.log)-1].Term)
		rf.role = Leader
		for i := 0; i < len(rf.nextIndex); i++ {
			rf.nextIndex[i] = len(rf.log)
			rf.matchIndex[i] = 0
		}
		go rf.SendHeartBeats()
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (2A)
		// Check if a leader election should be started.
		// rdTimeout := GetElectRandomTimeout()
		//DPrintf(dVote, "server:%v,选举超时时间:%v,未收到任何消息经过的时间:%d\n", rf.me, rdTimeout, time.Since(rf.timeStamp).Milliseconds())
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.mu.Lock()
		if rf.role != Leader && time.Since(rf.timeStamp) > time.Duration(ElectTimeout)*time.Millisecond {
			go rf.Elect()
		}
		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	DPrintf(dInfo, "server 调用Make启动%v", me)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.votedFor = -1
	rf.currentTerm = 0

	rf.applyCh = applyCh
	rf.log = make([]Entry, 0)
	rf.log = append(rf.log, Entry{Term: -1})
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.role = Follower
	rf.timeStamp = time.Now()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
