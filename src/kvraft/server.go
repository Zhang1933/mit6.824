package kvraft

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const (
	HandleOpTimeOut = time.Millisecond * 500
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

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	// Your definitions here.
	CltMaxSeqRes   map[int64]result    // 存储客户端Identifier最近的一次结果
	CommitResultCh map[int]chan result //客户端接收resualt管道
	db             map[string]string
}

type result struct {
	LastSeq uint64
	Value   string
	ResTerm int
	Err     Err
}

func (kv *KVServer) HandleOp(opArgs Op) (res result) {
	idx, term, isleader := kv.rf.Start(opArgs)
	if !isleader {
		return result{Err: ErrWrongLeader}
	}
	DPrintf(dServer, "leader server %v收到请求op:%+v,idx%v\n", kv.me, opArgs, idx)
	newch := make(chan result)
	kv.mu.Lock()
	kv.CommitResultCh[idx] = newch
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.CommitResultCh, idx)
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
			DPrintf(dServer, "server %v,op:%+v,Start Term%v,msg term %v:通道返回结果\n", kv.me, opArgs, term, msg.ResTerm)
			res = msg
			return
		} else {
			// 通道被关闭了，TODO:有这种情况？
			DPrintf(dServer, "server %v identifier %v Seq %v: 通道已经关闭, 有另一个协程收到了消息或更新的RPC覆盖, args.OpType=%v, args.Key=%v", kv.me, opArgs.Identifier, opArgs.Seq, opArgs.OPtype, opArgs.Key)
			res.Err = ErrWrongLeader
		}
	}
	return
}

func (kv *KVServer) DBExecute(op Op) (res result) {
	// 执行数据库操作
	DPrintf(ddbexe, "server %v DB执行请求op%+v\n", kv.me, op)
	res.LastSeq = op.Seq
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
			DPrintf(dTrace, "server%v收到raft commit的op%+v,CommandIndex%v\n", kv.me, op, log.CommandIndex)
			var res result
			if exres, exist := kv.CltMaxSeqRes[op.Identifier]; exist {
				if exres.LastSeq == op.Seq {
					newreq = false
				}
			}
			if newreq {
				res = kv.DBExecute(op)
				kv.CltMaxSeqRes[op.Identifier] = res
			} else {
				DPrintf(dTrace, "server%v:重复的请求,op:+%v\n", kv.me, op)
				res = kv.CltMaxSeqRes[op.Identifier]
			}
			_, isleader := kv.rf.GetState()
			if isleader {
				// 返回结果
				ch, exist := kv.CommitResultCh[log.CommandIndex]
				if exist {
					DPrintf(dTrace, "leader %v ApplyHandler 返回op结果%+v,idx%v\n", kv.me, op, log.CommandIndex)
					// 先解锁，再发数据，不然恰好超时时，另一边没人接受数据并且关闭不了管道
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
					DPrintf(dWarn, "leader %v ApplyHandler 发现%+v的管道不存在, 应该是超时被关闭了", kv.me, op)
				}
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_, isleader := kv.rf.GetState()
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	opArgs := Op{OPtype: OPGet, Key: args.Key, Seq: args.Seq, Identifier: args.Identifier}
	res := kv.HandleOp(opArgs)
	reply.Err = res.Err
	reply.Value = res.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, isleader := kv.rf.GetState()
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}
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
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.CltMaxSeqRes = make(map[int64]result)
	kv.CommitResultCh = make(map[int]chan result)
	kv.db = make(map[string]string)
	go kv.ApplyHandler()
	return kv
}
