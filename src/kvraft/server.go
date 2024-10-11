package kvraft

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const (
	HandleOpTimeOut = time.Millisecond * (raft.ElectTimeout)
)

type OPtype int

const (
	OPGet OPtype = iota
	OPPut
	OPAppend
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OPtype     OPtype
	Key        string
	Val        string
	Seq        uint64
	Identifier int64
}

type Pair struct {
	First  int64
	Second uint64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	// Your definitions here.
	persister   *raft.Persister
	lastapplied int // raft最近的apply

	CltMaxSeqRes   map[int64]result     // 存储客户端Identifier最近的一次结果
	CommitResultCh map[Pair]chan result //客户端接收resualt管道
	db             map[string]string
}

type result struct {
	LastSeq    uint64
	Identifier int64
	Value      string
	ResTerm    int
	Err        Err
}

func (kv *KVServer) GenSnapShot() []byte {
	// 调用时必须持有锁mu
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.db)
	e.Encode(kv.CltMaxSeqRes)

	serverState := w.Bytes()
	return serverState
}

func (kv *KVServer) LoadSnapshot(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if snapshot == nil || len(snapshot) < 1 {
		DPrintf(dTrace, "server %v LoadSnapshot快照为空", kv.me)
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	tmpdb := make(map[string]string)
	tmpCltMaxSeqRes := make(map[int64]result)
	if d.Decode(&tmpdb) != nil || d.Decode(&tmpCltMaxSeqRes) != nil {
		DPrintf(dError, "server %v快照加载失败", kv.me)
	} else {
		kv.db = tmpdb
		kv.CltMaxSeqRes = tmpCltMaxSeqRes
		DPrintf(dInfo, "server %v快照加载成功", kv.me)
	}
	if len(kv.db) == 0 {
		DPrintf(dWarn, "加载快照后db为空")
	}
}

func (kv *KVServer) DBExecute(op Op) (res result) {
	// 执行数据库操作
	DPrintf(ddbexe, "server %v DB执行请求op%+v\n", kv.me, op)
	res.LastSeq = op.Seq
	res.Identifier = op.Identifier
	switch op.OPtype {
	case OPGet:
		val, exist := kv.db[op.Key]
		if exist {
			res.Value = val
			res.Err = OK
			DPrintf(ddbexe, "server%vOPGet请求执行成功,res.Value:%v\n", kv.me, res.Value)
		} else {
			res.Err = ErrNoKey
			res.Value = ""
			DPrintf(ddbexe, "server%vOPGet请求No key\n", kv.me)
		}
	case OPPut:
		kv.db[op.Key] = op.Val
		res.Err = OK
		DPrintf(ddbexe, "server%vPUT请求执行成功,Value:%v\n", kv.me, op.Val)
	case OPAppend:
		val, exsit := kv.db[op.Key]
		if exsit {
			kv.db[op.Key] = val + op.Val
			res.Err = OK
			DPrintf(ddbexe, "server%vAPPEND请求执行成功,Value:%v\n", kv.me, kv.db[op.Key])
		} else {
			kv.db[op.Key] = op.Val
			res.Err = OK
			DPrintf(ddbexe, "server%vAPPEND执行键不存在的情况,Value:%v\n", kv.me, op.Val)
		}
	}
	return
}

func (kv *KVServer) ApplyHandler() {
	for !kv.killed() {
		log := <-kv.applyCh
		if log.CommandValid {
			op := log.Command.(Op)
			newreq := true
			kv.mu.Lock()
			if kv.lastapplied >= log.CommandIndex {
				// db中已经执行过该命令了
				kv.mu.Unlock()
				continue
			}
			DPrintf(dTrace, "server %v收到raft commit的op%+v,CommandIndex%v,lastapplied%v\n", kv.me, op, log.CommandIndex, kv.lastapplied)
			kv.lastapplied = log.CommandIndex
			var res result
			if exres, exist := kv.CltMaxSeqRes[op.Identifier]; exist {
				if exres.LastSeq == op.Seq {
					newreq = false
				}
			}
			if newreq {
				res = kv.DBExecute(op)
				res.ResTerm = log.SnapshotTerm
				kv.CltMaxSeqRes[op.Identifier] = res
			} else {
				DPrintf(dTrace, "server%v:重复的请求,op:+%v\n", kv.me, op)
				res = kv.CltMaxSeqRes[op.Identifier]
			}
			// NOTE:不能getstatue判断是否为leader，当恰好在install快照时会死锁。
			ch, exist := kv.CommitResultCh[Pair{First: op.Identifier, Second: op.Seq}]
			if exist {
				DPrintf(dTrace, "leader %v ApplyHandler 返回op结果%+v,idx%v\n", kv.me, op, log.CommandIndex)
				// NOTE:先解锁，再发数据，不然恰好超时时，另一边没人接受数据并且关闭不了管道
				kv.mu.Unlock()
				func() {
					defer func() {
						if recover() != nil {
							// 如果这里有 panic，是因为通道关闭
							DPrintf(dTrace, "leader %v ApplyHandler:%+v的管道不存在, 应该是超时被关闭了", kv.me, op)
						}
					}()
					ch <- res
				}()
				kv.mu.Lock()
			} else {
				// 可能超时了，通道关闭
				DPrintf(dTimer, "server %v ApplyHandler 发现%+v的管道不存在, 应该是超时被关闭了", kv.me, op)
			}
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate/100*95 {
				DPrintf(dSnap, "server %v size:%v超过阈值%v,申请快照,log.CommandIndex%v", kv.me, kv.persister.RaftStateSize(), kv.maxraftstate, log.CommandIndex)
				snapShot := kv.GenSnapShot()
				kv.rf.Snapshot(log.CommandIndex, snapShot)
			}
			kv.mu.Unlock()
		} else if log.SnapshotValid {
			// 如果是一个快照
			kv.mu.Lock()
			// NOTE:发送快照的包，有可能网络延迟，收到旧快照
			if log.SnapshotIndex < kv.lastapplied {
				DPrintf(dWarn, "server %v,db中已经包含新的快照log.SnapshotIndex:%v,kv.lastapplied%v\n", kv.me, log.SnapshotIndex, kv.lastapplied)
				kv.mu.Unlock()
				continue
			}
			DPrintf(dSnap, "server %v ApplyHandler收到一个快照,应用快照SnapshotIndex:%v,lastapplied%v\n", kv.me, log.SnapshotIndex, kv.lastapplied)
			kv.lastapplied = log.SnapshotIndex
			kv.mu.Unlock()
			kv.LoadSnapshot(log.Snapshot)
		}
	}
}

func (kv *KVServer) HandleOp(opArgs Op) (res result) {
	kv.mu.Lock()
	if exres, exist := kv.CltMaxSeqRes[opArgs.Identifier]; exist {
		if exres.LastSeq == opArgs.Seq {
			// 重复的请求，直接返回
			DPrintf(dServer, "server %v收到重复的请求op%+v,直接返回\n", kv.me, opArgs)
			kv.mu.Unlock()
			return exres
		}
	}
	kv.mu.Unlock()
	idx, startTerm, isleader := kv.rf.Start(opArgs)
	DPrintf(dServer, "server %v收到新请求op:%+v,idx%v\n", kv.me, opArgs, idx)
	if !isleader {
		return result{Err: ErrWrongLeader}
	}
	newch := make(chan result)
	pii := Pair{First: opArgs.Identifier, Second: opArgs.Seq}
	kv.mu.Lock()
	kv.CommitResultCh[pii] = newch
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.CommitResultCh, pii)
		close(newch)
		kv.mu.Unlock()
	}()

	select {
	case <-time.After(HandleOpTimeOut):
		DPrintf(dTimer, "server %v操作超时 op:%+v\n", kv.me, opArgs)
		res.Err = ErrWrongLeader
		return
	case msg, success := <-newch:
		if success {
			DPrintf(dServer, "server %v,op:%+v,Start Term%v,msg term %v:通道返回结果\n", kv.me, opArgs, startTerm, msg.ResTerm)
			res = msg
			return
		} else {
			DPrintf(dError, "server%v通道未知关闭%+v", kv.me, opArgs)
			res.Err = ErrWrongLeader
		}
	}
	return
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	opArgs := Op{OPtype: OPGet, Key: args.Key, Seq: args.Seq, Identifier: args.Identifier}
	res := kv.HandleOp(opArgs)
	reply.Err = res.Err
	reply.Value = res.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	opArgs := Op{OPtype: OPAppend, Key: args.Key, Val: args.Value, Seq: args.Seq, Identifier: args.Identifier}
	if args.Op == "Put" {
		opArgs.OPtype = OPPut
	}
	res := kv.HandleOp(opArgs)
	reply.Err = res.Err
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.persister = persister
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.CltMaxSeqRes = make(map[int64]result)
	kv.CommitResultCh = make(map[Pair]chan result)
	kv.db = make(map[string]string)

	kv.LoadSnapshot(persister.ReadSnapshot())
	go kv.ApplyHandler()
	return kv
}
