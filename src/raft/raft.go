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

	heartTimer  *time.Timer
	currentTerm int
	votedFor    int
	log         []Entry

	condApply   *sync.Cond
	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	timeStamp time.Time
	role      int

	voteCount int //获得的票数
	applyCh   chan ApplyMsg

	snapshot          []byte
	lastIncludedIndex int // the snapshot replaces all entries up through and including this index
	lastIncludedTerm  int // term of lastIncludedIndex
}

func (rf *Raft) realidx2virtualidx(realidx int) int {
	// 使用前需用锁
	return realidx + rf.lastIncludedIndex
}

func (rf *Raft) virtual2realidx(viridx int) int {
	// 使用前需用锁
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
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
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
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&votedFor) != nil ||
		d.Decode(&currentTerm) != nil || d.Decode(&log) != nil || d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		DPrintf(dPersist, "server%vreadPersist失败,len(log)%v\n", rf.me, len(log))
	} else {
		rf.votedFor = votedFor
		rf.currentTerm = currentTerm
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
		DPrintf(dPersist, "server%vreadPersist成功,len(log)%v\n", rf.me, len(log))
	}
}

func (rf *Raft) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	rf.snapshot = data
	DPrintf(dSnap, "server %v 读取快照成功\n", rf.me)
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	DPrintf(dSnap, "向server%v申请Snapshot,idx%v\n", rf.me, index)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.lastApplied < index || index < rf.lastIncludedIndex {
		DPrintf(dSnap, "server %v拒绝,lastApplied:%v,index:%v,lastIncludedIndex:%v\n", rf.me, rf.lastApplied, index, rf.lastIncludedIndex)
		return
	}

	rf.snapshot = snapshot
	// 截断log
	rf.lastIncludedTerm = rf.log[rf.virtual2realidx(index)].Term
	rf.log = rf.log[rf.virtual2realidx(index):] // index 占位

	DPrintf(dSnap, "server %v 同意了Snapshot请求, 快照前的lastIncludedIndex=%v,快照后的lastidx:%v,log len:%v\n", rf.me, rf.lastIncludedIndex, index, len(rf.log))
	rf.lastIncludedIndex = index
	rf.persist()

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
			if (args.LastLogTerm > rf.log[len(rf.log)-1].Term) || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= rf.realidx2virtualidx(len(rf.log)-1)) {
				// 同意，投票
				reply.VoteGranted = true
				rf.votedFor = args.CandidateId
				rf.role = Follower
				rf.timeStamp = time.Now()
				DPrintf(dVote, "server %v 同意向 server %v投票,len(log)%v,lastlogterm:%v,lastidx%v\n", rf.me, args.CandidateId, len(rf.log), rf.log[len(rf.log)-1].Term, rf.lastIncludedIndex)
			} else {
				DPrintf(dVote, "日志不够新,server %v 拒绝向server %v投票 lastidx:%v\n", rf.me, args.CandidateId, rf.lastIncludedIndex)
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
	Xlen    int //  log length ,虚拟长度
}

func (rf *Raft) FindTermFirstIdx(Xterm int) int {
	// 从right开始,找到日志中第一个Xterm对应的idx,使用前需锁
	i := len(rf.log) - 1
	for i >= 0 && rf.log[i].Term >= Xterm {
		i--
	}
	return rf.realidx2virtualidx(i + 1)
}

type InstallSnapshotArgs struct {
	Term             int
	Leaderid         int
	Lastincludeidx   int
	LastIncludedTerm int
	Data             []byte
	Firstlog         Entry // 创建快照后，第一个日志项的log
}
type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) handleInstallSnapshot(serverTo int, argsP *InstallSnapshotArgs) {
	// 向serverTo 发送快照
	reply := InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(serverTo, argsP, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.timeStamp = time.Now()
		rf.role = Follower
		rf.votedFor = -1
		rf.persist()
		return
	}
	rf.nextIndex[serverTo] = rf.lastIncludedIndex + 1
}

func (rf *Raft) handleHeartBeat(serverTo int, argsP *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(serverTo, argsP, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader || reply.Term != rf.currentTerm {
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
				DPrintf(dLog2, "leader:%v处理日志缺失,server:%v,argsP.PrevLogIndex:%v,reply.Xlen:%v\n", rf.me, serverTo, argsP.PrevLogIndex, reply.Xlen)
			} else if reply.Term > 0 {
				// 说明是日志项不匹配
				rf.nextIndex[serverTo] = reply.XIndex
				DPrintf(dLog2, "leader:%v处理日志冲突,server:%v,argsP.PrevLogIndex:%v,Xterm:%v,Xidx:%v\n", rf.me, serverTo, argsP.PrevLogIndex, reply.Xterm, reply.XIndex)
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

	N := rf.realidx2virtualidx(len(rf.log) - 1)
	for N > rf.commitIndex {
		cnt := 1
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= N && rf.log[rf.virtual2realidx(N)].Term == rf.currentTerm {
				cnt += 1
			}
		}
		if cnt >= (len(rf.peers)+1)/2 {
			break
		}
		N -= 1
	}
	rf.commitIndex = N
	rf.condApply.Signal()

	rf.persist()
}

func (rf *Raft) CommitChecker() {
	// 检查是否有新的commit
	DPrintf(dCommit, "server %v 的 CommitChecker 开始运行", rf.me)
	for !rf.killed() {
		rf.mu.Lock()
		// DPrintf("server %v CommitChecker 获取锁mu", rf.me)
		for rf.commitIndex <= rf.lastApplied {
			rf.condApply.Wait()
		}
		msgBuf := make([]*ApplyMsg, 0, rf.commitIndex-rf.lastApplied)
		tmpApplied := rf.lastApplied
		for rf.commitIndex > tmpApplied {
			tmpApplied += 1
			if tmpApplied <= rf.lastIncludedIndex {
				// tmpApplied可能是snapShot中已经被截断的日志项, 这些日志项就不需要再发送了
				continue
			}
			if rf.virtual2realidx(tmpApplied) >= len(rf.log) {
				DPrintf(dCommit, "server %v CommitChecker数组越界: tmpApplied=%v,  rf.RealLogIdx(tmpApplied)=%v>=len(rf.log)=%v, lastIncludedIndex=%v", rf.me, tmpApplied, rf.virtual2realidx(tmpApplied), len(rf.log), rf.lastIncludedIndex)
			}
			msg := &ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.virtual2realidx(tmpApplied)].Command,
				CommandIndex: tmpApplied,
				SnapshotTerm: rf.log[rf.virtual2realidx(tmpApplied)].Term,
			}

			msgBuf = append(msgBuf, msg)
		}
		rf.mu.Unlock()
		// DPrintf("server %v CommitChecker 释放锁mu", rf.me)

		// 注意, 在解锁后可能又出现了SnapShot进而修改了rf.lastApplied
		for _, msg := range msgBuf {
			rf.mu.Lock()
			if msg.CommandIndex != rf.lastApplied+1 {
				rf.mu.Unlock()
				continue
			}
			DPrintf(dCommit, "server %v 准备commit, log = %v:%v, lastIncludedIndex=%v", rf.me, msg.CommandIndex, msg.SnapshotTerm, rf.lastIncludedIndex)

			rf.mu.Unlock()
			// 注意, 在解锁后可能又出现了SnapShot进而修改了rf.lastApplied

			rf.applyCh <- *msg

			rf.mu.Lock()
			if msg.CommandIndex != rf.lastApplied+1 {
				rf.mu.Unlock()
				continue
			}
			rf.lastApplied = msg.CommandIndex
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) SendHeartBeats() {
	for !rf.killed() {
		<-rf.heartTimer.C
		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}
		DPrintf(dLeader, "leader %v 发送心跳Term:%v,leader log len:%v,lastlogterm:%v,lastidx:%v,lastapply%v\n", rf.me, rf.currentTerm, len(rf.log), rf.log[len(rf.log)-1].Term, rf.lastIncludedIndex, rf.lastApplied)
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			if rf.nextIndex[i]-1 < rf.lastIncludedIndex {
				// TODO: rf.nextIndex[i] - 1 < rf.lastincludeidx,发送快照
				DPrintf(dSnap, "leader%v,server%v落后,发送快照rf.lastIncludedIndex%v,nextIndex%v\n", rf.me, i, rf.lastIncludedIndex, rf.nextIndex[i])
				args := InstallSnapshotArgs{
					Term:             rf.currentTerm,
					Leaderid:         rf.me,
					Lastincludeidx:   rf.lastIncludedIndex,
					LastIncludedTerm: rf.lastIncludedTerm,
					Data:             rf.snapshot,
					Firstlog:         rf.log[0],
				}
				go rf.handleInstallSnapshot(i, &args)
			} else {
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[i] - 1,
					PrevLogTerm:  rf.log[rf.virtual2realidx(rf.nextIndex[i]-1)].Term,
					LeaderCommit: rf.commitIndex,
				}
				if rf.realidx2virtualidx(len(rf.log)) > rf.nextIndex[i] {
					args.Entries = rf.log[rf.virtual2realidx(rf.nextIndex[i]):]
					DPrintf(dCommit, "leader %v 向 server %v 添加新的Entries,leader log len:%v,follower nextidx:%v\n", rf.me, i, len(rf.log), rf.nextIndex[i])
				}
				go rf.handleHeartBeat(i, &args)
			}
		}
		rf.mu.Unlock()
		rf.heartTimer.Reset(time.Duration(HeartBeatTimeOut) * time.Millisecond)
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
	DPrintf(dLog, "server %v 收到leader %v的心跳,serverTerm%v,len(log)%v,lastlogterm%v,lastidx%v,argprevidx:%v\n", rf.me, argsP.LeaderId, rf.currentTerm, len(rf.log), rf.log[len(rf.log)-1].Term, rf.lastIncludedIndex, argsP.PrevLogIndex)
	rf.timeStamp = time.Now()

	if argsP.Term > rf.currentTerm {
		rf.currentTerm = argsP.Term
		rf.role = Follower
		rf.votedFor = -1
		rf.persist()
	}
	if rf.virtual2realidx(argsP.PrevLogIndex) >= len(rf.log) {
		// 过长
		DPrintf(dLog, "server%v处理leader%v过长,serverTerm%v,len(log)%v,lastlogterm%v,lastidx%v\n", rf.me, argsP.LeaderId, rf.currentTerm, len(rf.log), rf.log[len(rf.log)-1].Term, rf.lastIncludedIndex)
		replyP.Xterm = -1
		replyP.Xlen = rf.realidx2virtualidx(len(rf.log))
		replyP.Term = rf.currentTerm
		replyP.Success = false
		return
	}
	if rf.virtual2realidx(argsP.PrevLogIndex) < 0 {
		// 已经被快照过了
		replyP.Success = true
		replyP.Term = rf.currentTerm
		return
	}
	if rf.log[rf.virtual2realidx(argsP.PrevLogIndex)].Term != argsP.PrevLogTerm {

		replyP.Xterm = rf.log[rf.virtual2realidx(argsP.PrevLogIndex)].Term
		replyP.XIndex = rf.FindTermFirstIdx(replyP.Xterm)
		replyP.Term = rf.currentTerm
		replyP.Success = false
		DPrintf(dLog, "日志项不匹配server %v,leader %v,argsP.PrevLogIndex:%v,rf.log previdx term:%v,argsPrevTerm:%v,Xterm:%v,Xidx:%v\n", rf.me, argsP.LeaderId, argsP.PrevLogIndex, rf.log[rf.virtual2realidx(argsP.PrevLogIndex)].Term, argsP.PrevLogTerm, replyP.Xterm, replyP.XIndex)
		return
	}
	if argsP.Entries != nil {
		// 3. If an existing entry conflicts with a new one (same index
		// but different terms), delete the existing entry and all that
		// follow it (§5.3)
		// 4. Append any new entries not already in the log ，防止同一个log重复添加

		for idx, log := range argsP.Entries {
			ridx := rf.virtual2realidx(argsP.PrevLogIndex) + 1 + idx
			if ridx < len(rf.log) && rf.log[ridx].Term != log.Term {
				// 某位置发生了冲突, 覆盖这个位置开始的所有内容
				rf.log = rf.log[:ridx]
				rf.log = append(rf.log, argsP.Entries[idx:]...)
				break
			} else if ridx == len(rf.log) {
				// 没有发生冲突但长度更长了, 直接拼接
				rf.log = append(rf.log, argsP.Entries[idx:]...)
				break
			}
		}

		DPrintf(dLog2, "server:%v添加日志项len: %v,lastlogterm%v,lastidx%v\n", rf.me, len(rf.log), rf.log[len(rf.log)-1].Term, rf.lastIncludedIndex)
		rf.persist()
	}
	if argsP.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(argsP.LeaderCommit), float64(rf.realidx2virtualidx(len(rf.log)-1))))
		rf.condApply.Signal()
	}
	replyP.Success = true
	replyP.Term = rf.currentTerm
}

func (rf *Raft) InstallSnapshotReceive(argP *InstallSnapshotArgs, replyP *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(dSnap, "server%v收到%v的InstallSnapshot,argLastidx%v,lastidx%v,log len%v\n", rf.me, argP.Leaderid, argP.Lastincludeidx, rf.lastIncludedIndex, len(rf.log))
	if argP.Term < rf.currentTerm {
		DPrintf(dSnap, "server %v 拒绝来自 %v 的 InstallSnapshot, 更小的Term,term:%v,argP.Term%v\n", rf.me, argP.Leaderid, rf.currentTerm, argP.Term)
		replyP.Term = rf.currentTerm
		return
	}
	rf.timeStamp = time.Now()
	if argP.Term > rf.currentTerm {
		rf.role = Follower
		rf.currentTerm = argP.Term
		rf.votedFor = -1
	}
	if (argP.Lastincludeidx >= rf.lastIncludedIndex && argP.Lastincludeidx < rf.realidx2virtualidx(len(rf.log))) && (rf.log[rf.virtual2realidx(argP.Lastincludeidx)].Term == argP.LastIncludedTerm) {
		// snapshot index 在log中间的情况
		//6.If existing log entry has same index and term as snapshot’s last included entry, retain log entries following it and reply
		DPrintf(dSnap, "server%v,leader%v,snapshot index 在log中间的情况\n", rf.me, argP.Leaderid)
		rf.log = rf.log[rf.virtual2realidx(argP.Lastincludeidx):]
	} else {
		// snapshot index
		//7. Discard the entire log
		rf.log = make([]Entry, 0)
		rf.log = append(rf.log, argP.Firstlog)
	}
	replyP.Term = rf.currentTerm
	if argP.Lastincludeidx == rf.lastIncludedIndex && argP.LastIncludedTerm == rf.lastIncludedTerm {
		// 已经有快照了，不重复应用快照
		return
	}
	//8.Reset state machine using snapshot contents (and load snapshot’s cluster configuration)
	rf.snapshot = argP.Data
	rf.lastIncludedIndex = argP.Lastincludeidx
	rf.lastIncludedTerm = argP.LastIncludedTerm
	if rf.commitIndex < rf.lastIncludedIndex {
		rf.commitIndex = rf.lastIncludedIndex
	}
	if rf.lastApplied < rf.lastIncludedIndex {
		rf.lastApplied = rf.lastIncludedIndex
	}

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      argP.Data,
		SnapshotTerm:  argP.LastIncludedTerm,
		SnapshotIndex: argP.Lastincludeidx,
	}
	rf.applyCh <- msg
	rf.persist()
	DPrintf(dSnap, "server%v应用快照成功,lastidx%v,log len%v\n", rf.me, rf.lastIncludedIndex, len(rf.log))
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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshotReceive", args, reply)
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
	defer rf.heartTimer.Reset(time.Duration(1) * time.Millisecond)
	if rf.role != Leader {
		return -1, -1, false
	}
	rf.log = append(rf.log, Entry{Term: rf.currentTerm, Command: command})
	rf.persist()
	return rf.realidx2virtualidx(len(rf.log) - 1), rf.currentTerm, true
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
		LastLogIndex: rf.realidx2virtualidx(len(rf.log) - 1),
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	DPrintf(dVote, "server %v发起公投,Term:%v,len(log):%v,lastlogterm%v,lastidx%v\n", rf.me, rf.currentTerm, len(rf.log), rf.log[len(rf.log)-1].Term, rf.lastIncludedIndex)
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
	if rf.role == Candidate {
		if rf.voteCount == len(rf.peers)/2+1 {
			DPrintf(dVote, "Server %v 成为Leader,log len%v,lastlogterm%v,lastidx%v", rf.me, len(rf.log), rf.log[len(rf.log)-1].Term, rf.lastIncludedIndex)
			rf.role = Leader
			for i := 0; i < len(rf.nextIndex); i++ {
				rf.nextIndex[i] = rf.realidx2virtualidx(len(rf.log))
				rf.matchIndex[i] = 0
			}
			rf.heartTimer = time.NewTimer(1)
			go rf.SendHeartBeats()
		}
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
	rf.condApply = sync.NewCond(&rf.mu)

	rf.log = make([]Entry, 0)
	rf.log = append(rf.log, Entry{Term: 0})
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.role = Follower
	rf.timeStamp = time.Now()
	rf.heartTimer = time.NewTimer(0)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.CommitChecker()

	return rf
}
