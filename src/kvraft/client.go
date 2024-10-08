package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.identifier = nrand()

	return ck
}

func (ck *Clerk) getseq() (Seqres uint64) {
	Seqres = ck.seq
	ck.seq += 1
	return
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{Key: key, Seq: ck.getseq(), Identifier: ck.identifier}
	reply := GetReply{}
	DPrintf(dClient, "client%v发送操作请求Get,op:%+v\n", ck.identifier, args)
	for {
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			DPrintf(dClient, "client %v,reply.Err %+v,ck.leaderId%v,op seq%v\n", ck.identifier, reply.Err, ck.leaderId, args.Seq)
			ck.leaderId += 1
			ck.leaderId %= len(ck.servers)
			continue
		}
		switch reply.Err {
		case ErrChanClose:
			continue
		case ErrNoKey:
			DPrintf(dClient, "ErrNoKey client %v,reply.Err %+v,op seq%v\n", ck.identifier, reply.Err, args.Seq)
			return reply.Value
		}
		return reply.Value
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{Key: key, Value: value, Op: op, Seq: ck.getseq(), Identifier: ck.identifier}
	reply := PutAppendReply{}
	DPrintf(dClient, "client%v发送操作请求PutAppend,op:%+v\n", ck.identifier, args)
	for {
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			DPrintf(dClient, "client %v,reply.Err %+v,ck.leaderId%v,op seq%v\n", ck.identifier, reply.Err, ck.leaderId, args.Seq)
			ck.leaderId += 1
			ck.leaderId %= len(ck.servers)
			continue
		}
		switch reply.Err {
		case ErrChanClose:
			continue
		case ErrUndefine:
			DPrintf(dClient, "ErrUndefine client %v,reply.Err %+v,op seq%v\n", ck.identifier, reply.Err, args.Seq)
			continue
		}
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
