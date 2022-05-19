package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
	"fmt"
	"log"
)
import "time"
import "crypto/rand"
import "math/big"
const RetryWait = 100 * time.Millisecond
type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
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
	// Your code here.
	ck.clientId = nrand()
	ck.requestId = 0
	ck.leaderId = 0
	ck.Debug = false
	return ck
}

func (ck *Clerk) incrementRequestId() {
	ck.requestId = (ck.requestId + 1) % (1 << 62)
}

func (ck *Clerk) Query(num int) Config {
	ck.incrementRequestId()
	args := QueryArgs{
		Num:       num,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}
	leaderId := ck.leaderId
	for {
		// try each known server.
		ck.log("send query, %+v, leaderId: %d", args, leaderId)
		reply := QueryReply{}
		ok := ck.servers[leaderId].Call("ShardCtrler.Query", &args, &reply)
		if ok && reply.WrongLeader == false {
			ck.leaderId = leaderId
			return  reply.Config
		}
		leaderId = (leaderId + 1) % len(ck.servers)
		time.Sleep(RetryWait)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.incrementRequestId()
	args := JoinArgs{
		Servers:   servers,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}
	leaderId := ck.leaderId
	for {
		// try each known server.
		ck.log("send join, %+v, leaderId: %d", args, leaderId)
		reply := JoinReply{}
		ok := ck.servers[leaderId].Call("ShardCtrler.Join", &args, &reply)
		if ok && reply.WrongLeader == false {
			ck.leaderId = leaderId
			return
		}
		leaderId = (leaderId + 1) % len(ck.servers)
		time.Sleep(RetryWait)
	}
}

func (ck *Clerk) Leave(gids []int) {

	ck.incrementRequestId()
	args := LeaveArgs{
		GIDs:      gids,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}
	leaderId := ck.leaderId
	for {
		ck.log("send leave, %+v", args)
		// try each known server.
		reply := LeaveReply{}
		ok := ck.servers[leaderId].Call("ShardCtrler.Leave", &args, &reply)
		if ok && reply.WrongLeader == false {
			ck.leaderId = leaderId
			return
		}
		leaderId = (leaderId + 1) % len(ck.servers)
		time.Sleep(RetryWait)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := MoveArgs{
		Shard:     shard,
		GID:       gid,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}
	leaderId := ck.leaderId
	for {
		ck.log("send move, %+v", args)
		// try each known server.
		reply := JoinReply{}
		ok := ck.servers[leaderId].Call("ShardCtrler.Move", &args, &reply)
		if ok && reply.WrongLeader == false {
			ck.leaderId = leaderId
			return
		}
		leaderId = (leaderId + 1) % len(ck.servers)
		time.Sleep(RetryWait)
	}
}
func (ck *Clerk) log(format string, a ...interface{}) {
	if ck.Debug {
		r := fmt.Sprintf(format, a...)
		log.Printf("client:%d leaderid: %d, log: %s\n", ck.clientId, ck.leaderId, r)
	}
}