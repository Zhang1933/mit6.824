package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	seq        uint64
	identifier int64
	leaderId   int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (ck *Clerk) getseq() (Seqres uint64) {
	Seqres = ck.seq
	ck.seq += 1
	return
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.identifier = nrand()
	ck.seq = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{Num: num, Seq: ck.getseq(), Identifier: ck.identifier}
	for {
		// try each known server.
		for id, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && !reply.WrongLeader {
				DPrintf(dClient, "Query返回成功op:%+v reply%+v tryleader%v", args, reply, id)
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{Servers: servers, Seq: ck.getseq(), Identifier: ck.identifier}
	// Your code here.
	for {
		// try each known server.
		for id, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && !reply.WrongLeader {
				DPrintf(dClient, "Join返回成功op:%+v reply%+v tryleader%v", args, reply, id)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{GIDs: gids, Seq: ck.getseq(), Identifier: ck.identifier}
	// Your code here.
	for {
		// try each known server.
		for id, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && !reply.WrongLeader {
				DPrintf(dClient, "Leave返回成功op:%+v reply%+v tryleader%v", args, reply, id)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{Shard: shard, GID: gid, Seq: ck.getseq(), Identifier: ck.identifier}
	// Your code here.
	for {
		// try each known server.
		for id, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && !reply.WrongLeader {
				DPrintf(dClient, "Move返回成功op:%+v reply%+v tryleader%v", args, reply, id)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
