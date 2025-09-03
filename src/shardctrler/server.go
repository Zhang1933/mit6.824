package shardctrler

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type OPType string

const (
	OPJoin  OPType = "Join"
	OPLeave OPType = "Leave"
	OPMove  OPType = "Move"
	OPQuery OPType = "Query"

	// InvalidGid all shards should be assigned to GID zero (an invalid GID).
	InvalidGid = 0
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead int32 // set by Kill()

	lastapplied int // raft最近的apply

	CltMaxSeqRes   map[int64]result     // 存储客户端Identifier最近的一次结果
	CommitResultCh map[Pair]chan result //客户端接收resualt管道

	configs []Config // indexed by Config num
}

type result struct {
	LastSeq    uint64
	Identifier int64
	Err        Err
	ResTerm    int
	Config     Config
}

type Op struct {
	OpType OPType
	// All
	Seq        uint64
	Identifier int64
	// JoinArgs
	JoinServers map[int][]string
	// LeaveArgs
	LeaveGids []int
	// MoveArgs
	MoveShard int
	MoveGID   int
	// QueryArgs
	QueryNum int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	opArgs := Op{OpType: OPJoin, JoinServers: args.Servers, Seq: args.Seq, Identifier: args.Identifier}
	res := sc.HandleOp(opArgs)
	reply.Err = res.Err
	if res.Err == ErrWrongLeader {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	opArgs := Op{OpType: OPLeave, LeaveGids: args.GIDs, Seq: args.Seq, Identifier: args.Identifier}
	res := sc.HandleOp(opArgs)
	reply.Err = res.Err
	if res.Err == ErrWrongLeader {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	opArgs := Op{OpType: OPMove, Seq: args.Seq, Identifier: args.Identifier, MoveShard: args.Shard, MoveGID: args.GID}
	res := sc.HandleOp(opArgs)
	reply.Err = res.Err
	if res.Err == ErrWrongLeader {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	opArgs := Op{OpType: OPQuery, Seq: args.Seq, Identifier: args.Identifier, QueryNum: args.Num}
	res := sc.HandleOp(opArgs)
	reply.Err = res.Err
	if res.Err == ErrWrongLeader {
		reply.WrongLeader = true
	}
	reply.Config = res.Config
}

func (sc *ShardCtrler) HandleOp(opArgs Op) (res result) {
	sc.mu.Lock()
	if exres, exist := sc.CltMaxSeqRes[opArgs.Identifier]; exist {
		if exres.LastSeq == opArgs.Seq {
			// 重复的请求，直接返回
			DPrintf(dServer, "server %v收到重复的请求op%+v,直接返回\n", sc.me, opArgs)
			sc.mu.Unlock()
			return exres
		}
	}
	sc.mu.Unlock()
	idx, startTerm, isleader := sc.rf.Start(opArgs)
	if !isleader {
		return result{Err: ErrWrongLeader}
	}
	DPrintf(dServer, "leader %v收到新请求op:%+v,idx%v\n", sc.me, opArgs, idx)
	newch := make(chan result)
	pii := Pair{First: opArgs.Identifier, Second: opArgs.Seq}
	sc.mu.Lock()
	sc.CommitResultCh[pii] = newch
	sc.mu.Unlock()

	defer func() {
		sc.mu.Lock()
		delete(sc.CommitResultCh, pii)
		close(newch)
		sc.mu.Unlock()
	}()

	select {
	case <-time.After(HandleOpTimeOut):
		DPrintf(dTimer, "server %v操作超时 op:%+v\n", sc.me, opArgs)
		res.Err = ErrWrongLeader
		return
	case msg, success := <-newch:
		if success {
			DPrintf(dServer, "server %v,op:%+v,Start Term%v,msg term %v:通道返回结果\n", sc.me, opArgs, startTerm, msg.ResTerm)
			res = msg
			return
		} else {
			DPrintf(dError, "server%v通道未知关闭%+v", sc.me, opArgs)
			res.Err = ErrWrongLeader
		}
	}
	return
}

func (sc *ShardCtrler) ApplyHandler() {
	for !sc.killed() {
		log := <-sc.applyCh
		if log.CommandValid {
			op := log.Command.(Op)
			newreq := true
			sc.mu.Lock()
			if sc.lastapplied >= log.CommandIndex {
				// db中已经执行过该命令了
				sc.mu.Unlock()
				continue
			}
			DPrintf(dTrace, "server %v收到raft commit的op%+v,CommandIndex%v,lastapplied%v\n", sc.me, op, log.CommandIndex, sc.lastapplied)
			sc.lastapplied = log.CommandIndex
			var res result
			if exres, exist := sc.CltMaxSeqRes[op.Identifier]; exist {
				if exres.LastSeq == op.Seq {
					newreq = false
				}
			}
			if newreq {
				res = sc.ConfigExecute(op)
				res.ResTerm = log.SnapshotTerm
				sc.CltMaxSeqRes[op.Identifier] = res
			} else {
				DPrintf(dTrace, "server%v:重复的请求,op:+%v\n", sc.me, op)
				res = sc.CltMaxSeqRes[op.Identifier]
			}
			// NOTE:不能getstatue判断是否为leader，当恰好在install快照时会死锁。
			ch, exist := sc.CommitResultCh[Pair{First: op.Identifier, Second: op.Seq}]
			if exist {
				DPrintf(dTrace, "leader %v ApplyHandler 返回op结果%+v,idx%v\n", sc.me, op, log.CommandIndex)
				// NOTE:先解锁，再发数据，不然恰好超时时，另一边没人接受数据并且关闭不了管道
				sc.mu.Unlock()
				func() {
					defer func() {
						if recover() != nil {
							// 如果这里有 panic，是因为通道关闭
							DPrintf(dTrace, "leader %v ApplyHandler:%+v的管道不存在, 应该是超时被关闭了", sc.me, op)
						}
					}()
					ch <- res
				}()
				sc.mu.Lock()
			}
			// if sc.maxraftstate != -1 && sc.persister.RaftStateSize() >= sc.maxraftstate/100*95 {
			// 	DPrintf(dSnap, "server %v size:%v超过阈值%v,申请快照,log.CommandIndex%v", sc.me, sc.persister.RaftStateSize(), sc.maxraftstate, log.CommandIndex)
			// 	// snapShot := sc.GenSnapShot()
			// 	// sc.rf.Snapshot(log.CommandIndex, snapShot)
			// }
			sc.mu.Unlock()
		} else if log.SnapshotValid {
			// 如果是一个快照
			sc.mu.Lock()
			// NOTE:发送快照的包，有可能网络延迟，收到旧快照
			if log.SnapshotIndex < sc.lastapplied {
				DPrintf(dWarn, "server %v,db中已经包含新的快照log.SnapshotIndex:%v,kv.lastapplied%v\n", sc.me, log.SnapshotIndex, sc.lastapplied)
				sc.mu.Unlock()
				continue
			}
			DPrintf(dSnap, "server %v ApplyHandler收到一个快照,应用快照SnapshotIndex:%v,lastapplied%v\n", sc.me, log.SnapshotIndex, sc.lastapplied)
			sc.lastapplied = log.SnapshotIndex
			sc.mu.Unlock()
			// sc.LoadSnapshot(log.Snapshot)
		}
	}
}

func (sc *ShardCtrler) ConfigExecute(op Op) (res result) {
	// 调用时要求持有锁
	res.LastSeq = op.Seq
	res.Identifier = op.Identifier
	res.Err = OK
	switch op.OpType {
	case OPJoin:
		res.Config = *sc.JoinHandler(op.JoinServers)
		sc.configs = append(sc.configs, res.Config)
	case OPLeave:
		res.Config = *sc.LeaveHandler(op.LeaveGids)
		sc.configs = append(sc.configs, res.Config)
	case OPMove:
		res.Config = *sc.MoveHandler(op.MoveGID, op.MoveShard)
		sc.configs = append(sc.configs, res.Config)
	case OPQuery:
		res.Config = *sc.QueryHandler(op.QueryNum)
	}
	return
}

// 负载均衡
// GroupMap : gid -> servers[]
// lastShards : shard -> gid
func (sc *ShardCtrler) loadBanlance(GroupShardsCnt map[int]int, lastShards [NShards]int) [NShards]int {

	length := len(GroupShardsCnt)
	ave := NShards / length
	reminder := NShards % length
	DPrintf(dTrace, "server %v负载均衡前的lastshards%+v,ave:%v,reminder:%v,length:%v", sc.me, lastShards, ave, reminder, length)
	// 确定性排序,将GroupShardsCnt中的键按值进行从大到小排序
	sortedGidKeyList := make([]int, 0, len(GroupShardsCnt))
	for k := range GroupShardsCnt {
		sortedGidKeyList = append(sortedGidKeyList, k)
	}
	// 有序键
	sort.Slice(sortedGidKeyList, func(i, j int) bool {
		if GroupShardsCnt[sortedGidKeyList[i]] != GroupShardsCnt[sortedGidKeyList[j]] {
			return GroupShardsCnt[sortedGidKeyList[i]] > GroupShardsCnt[sortedGidKeyList[j]] // 按值从大到小排序
		}
		// 如果值相同，按键的值降序排序
		return sortedGidKeyList[i] > sortedGidKeyList[j]
	})
	tempreminder := reminder
	for i := 0; i < length; i++ {
		target := ave
		if tempreminder > 0 {
			target += 1
			tempreminder -= 1
		}
		// 释放超出负载的group
		gidNow := sortedGidKeyList[i]
		if GroupShardsCnt[gidNow] > target {
			changeNum := GroupShardsCnt[gidNow] - target
			for shard, gid := range lastShards {
				if changeNum <= 0 {
					break
				}
				if gid == gidNow {
					lastShards[shard] = InvalidGid
					changeNum--
				}
			}
			GroupShardsCnt[gidNow] = target
		}
	}
	tempreminder = reminder
	for i := 0; i < length; i++ {
		target := ave
		if tempreminder > 0 {
			target += 1
			tempreminder -= 1
		}
		gidNow := sortedGidKeyList[i]
		if GroupShardsCnt[gidNow] < target {
			changeNum := target - GroupShardsCnt[gidNow]
			for shard, gid := range lastShards {
				if changeNum <= 0 {
					break
				}
				if gid == InvalidGid {
					lastShards[shard] = gidNow
					changeNum--
				}
			}
			GroupShardsCnt[gidNow] = target
		}
	}
	DPrintf(dTrace, "server %v负载均衡后的lastshards%+v", sc.me, lastShards)
	return lastShards
}

/*
The Join RPC is used by an administrator to add new replica groups. Its argument is a set of mappings from unique, non-zero replica group identifiers (GIDs) to lists of server names. The shardctrler should react by creating a new configuration that includes the new replica groups. The new configuration should divide the shards as evenly as possible among the full set of groups, and should move as few shards as possible to achieve that goal. The shardctrler should allow re-use of a GID if it's not part of the current configuration (i.e. a GID should be allowed to Join, then Leave, then Join again).
*/
func (sc *ShardCtrler) JoinHandler(servers map[int][]string) (res *Config) {
	lastconfig := sc.configs[len(sc.configs)-1]
	newGroups := make(map[int][]string)
	DPrintf(dJoin, "server %v添加新的组,添加之前的配置%+v", sc.me, lastconfig)

	// 添加新的组
	for gid, serverlist := range lastconfig.Groups {
		newGroups[gid] = serverlist
	}
	for gid, serverlist := range servers {
		newGroups[gid] = serverlist
	}

	//记录每个组有多少个分片
	GroupShardsCnt := make(map[int]int)
	for gid := range newGroups {
		GroupShardsCnt[gid] = 0
	}
	for _, gid := range lastconfig.Shards {
		if gid != InvalidGid {
			GroupShardsCnt[gid] += 1
		}
	}
	if len(GroupShardsCnt) == 0 {
		// 没有Shards时
		DPrintf(dJoin, "server %v: 集群中没有存储Shards", sc.me)
		res = &Config{
			Num:    len(sc.configs),
			Shards: [NShards]int{},
			Groups: newGroups,
		}
	} else {
		// 需要负载均衡
		res = &Config{
			Num:    len(sc.configs),
			Shards: sc.loadBanlance(GroupShardsCnt, lastconfig.Shards),
			Groups: newGroups,
		}
	}

	DPrintf(dJoin, "server %v添加新的组,添加后的配置%+v", sc.me, res)
	return
}

/*The Leave RPC's argument is a list of GIDs of previously joined groups. The shardctrler should create a new configuration that does not include those groups, and that assigns those groups' shards to the remaining groups. The new configuration should divide the shards as evenly as possible among the groups, and should move as few shards as possible to achieve that goal.*/
func (sc *ShardCtrler) LeaveHandler(LeaveGids []int) (res *Config) {
	// 需要离开的组
	leaveGroupMap := make(map[int]bool)
	for _, gid := range LeaveGids {
		leaveGroupMap[gid] = true
	}
	lastConfig := sc.configs[len(sc.configs)-1]
	newGroups := make(map[int][]string)
	DPrintf(dLeave, "server %v删除组之前的配置%+v", sc.me, lastConfig)

	//deepcopy
	for gid, serverList := range lastConfig.Groups {
		newGroups[gid] = serverList
	}

	// 删除字段中对应的gid的键
	for _, gid := range LeaveGids {
		delete(newGroups, gid)
	}

	GroupShardCnt := make(map[int]int)
	for gid := range newGroups {
		GroupShardCnt[gid] = 0
	}
	newShard := lastConfig.Shards
	for shard, gid := range lastConfig.Shards {
		if gid != InvalidGid {
			if leaveGroupMap[gid] {
				newShard[shard] = InvalidGid
			} else {
				GroupShardCnt[gid] += 1
			}
		}
	}
	if len(GroupShardCnt) == 0 {
		res = &Config{
			Num:    len(sc.configs),
			Shards: [NShards]int{},
			Groups: newGroups,
		}
	} else {
		res = &Config{
			Num:    len(sc.configs),
			Shards: sc.loadBanlance(GroupShardCnt, newShard),
			Groups: newGroups,
		}
	}
	DPrintf(dLeave, "server %v删除组之后的配置%+v", sc.me, res)
	return
}

/*
The Move RPC's arguments are a shard number and a GID. The shardctrler should create a new configuration in which the shard is assigned to the group. The purpose of Move is to allow us to test your software. A Join or Leave following a Move will likely un-do the Move, since Join and Leave re-balance.
*/
func (sc *ShardCtrler) MoveHandler(MoveGID int, MoveShard int) (res *Config) {
	lastConfig := sc.configs[len(sc.configs)-1]
	DPrintf(dMove, "server %v Move shard%v到GID%v,Move之前的配置%+v", sc.me, MoveShard, MoveGID, lastConfig)
	res = &Config{
		Num:    len(sc.configs),
		Shards: [NShards]int{},
		Groups: make(map[int][]string),
	}
	//deepcopy
	res.Shards = lastConfig.Shards
	for gid, serversList := range lastConfig.Groups {
		res.Groups[gid] = serversList
	}
	res.Shards[MoveShard] = MoveGID

	DPrintf(dMove, "server %v Move shard%v到GID%v,Move之后的配置%+v", sc.me, MoveShard, MoveGID, res)
	return
}

/*
The Query RPC's argument is a configuration number. The shardctrler replies with the configuration that has that number. If the number is -1 or bigger than the biggest known configuration number, the shardctrler should reply with the latest configuration. The result of Query(-1) should reflect every Join, Leave, or Move RPC that the shardctrler finished handling before it received the Query(-1) RPC.
*/
func (sc *ShardCtrler) QueryHandler(QueryNum int) *Config {
	if QueryNum == -1 || QueryNum > len(sc.configs) {
		return &sc.configs[len(sc.configs)-1]
	}
	return &sc.configs[QueryNum]
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.CltMaxSeqRes = make(map[int64]result)
	sc.CommitResultCh = make(map[Pair]chan result)
	go sc.ApplyHandler()
	return sc
}
