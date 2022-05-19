package kvraft

import (
	"6.824/labrpc"
	"fmt"
	"log"
	"time"
)
import "crypto/rand"
import "math/big"


const RetryWait = 20 * time.Millisecond
type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId int64
	requestId int64
	leaderId int
	Debug    bool
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
	ck.clientId = nrand()
	ck.requestId = 0
	ck.leaderId = 0
	ck.Debug = false
	return ck
}

//
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
//
func (ck *Clerk) incrementRequestId() {
	ck.requestId = (ck.requestId + 1) % (1 << 62)
}

func (ck *Clerk) Get(key string) string {
	ck.incrementRequestId()
	leaderId := ck.leaderId
	args := GetArgs{
		Key:       key,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}
	ck.log("client get: key: %v", key)
	for  {
		reply := GetReply{}
		ok := ck.servers[leaderId].Call("KVServer.Get", &args, &reply)
		if !ok {
			ck.log("Client Get rpc failed, wait 20 millisecond and retry...")
			time.Sleep(RetryWait)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		}
		ck.log("client call. Method: Get, key: %v", args.Key)
		switch reply.Err {
		case OK:
			ck.log("get successfully, key: %v, value: %v, requestId: %v", key,reply.Value, args.RequestId)
			ck.leaderId = leaderId
			return reply.Value
		case ErrWrongLeader:
			ck.log("Method: Get, Wrong leader!")
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		case ErrNoKey:
			ck.log("Error key!")
			ck.leaderId = leaderId
			return ""
		case ErrTimeout:
			ck.log("Get timeout! Retry!")
			continue
		}
	}
	// You will have to modify this function.
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.incrementRequestId()
	leaderId := ck.leaderId
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}
	ck.log("client put append: key: %v, value: %v, op: %v", key, value, op)
	for {
		reply := PutAppendReply{}
		ok := ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			ck.log("Client PutAppend rpc failed, wait 20 millisecond and retry.")
			leaderId = (leaderId + 1) % len(ck.servers)
			time.Sleep(RetryWait)
			continue
		}
		ck.log("client call. Method: %v, key: %v value: %v, requestId: %v", args.Op, args.Key, args.Value, args.RequestId)
		switch reply.Err {
		case OK:
			ck.log("PutAppend OK.\n")
			ck.leaderId = leaderId
			return
		case ErrWrongLeader:
			ck.log("Method: PutAppend, Wrong leader.")
			leaderId = (leaderId + 1) % len(ck.servers)
			time.Sleep(RetryWait)
			continue
		case ErrTimeout:
			continue
		case ErrNoKey:
			ck.log("Method: PutAppend, Error No Key")
			ck.leaderId = leaderId
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}



func (ck *Clerk) log(format string, a ...interface{}) {
	if ck.Debug {
		r := fmt.Sprintf(format, a...)
		log.Printf("client:%d leaderid: %d, log: %s\n", ck.clientId, ck.leaderId, r)
	}
}

