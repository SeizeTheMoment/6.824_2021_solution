package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"6.824/labrpc"
	"fmt"
	"log"
)
import "crypto/rand"
import "math/big"
import "6.824/shardctrler"
import "time"

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
const RetryWait = 100 * time.Millisecond
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	clientId int64
	requestId int64
	leaderIds map[int]int           //gid -> leaderId 每一个group的leaderId
	Debug bool
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.requestId = 0
	ck.Debug = false
	ck.leaderIds = make(map[int]int)
	return ck
}

func (ck *Clerk) incrementRequestId() {
	ck.requestId = (ck.requestId + 1) % (1 << 62)
}
//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	ck.incrementRequestId()
	return ck.Operation(&OpCommandArgs{
		Op:        GET,
		Key:       key,
		Value:     "",
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	})
}

func (ck *Clerk) Put(key string, value string) {
	ck.incrementRequestId()
	ck.Operation(&OpCommandArgs{
		Op:        PUT,
		Key:       key,
		Value:     value,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	})
}
func (ck *Clerk) Append(key string, value string) {
	ck.incrementRequestId()
	ck.Operation(&OpCommandArgs{
		Op:        APPEND,
		Key:       key,
		Value:     value,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	})
}
func (ck *Clerk) Operation(args *OpCommandArgs) string {
	ck.log("client op: %+v", args)
	for  {
		shardId := key2shard(args.Key)
		gid := ck.config.Shards[shardId]

		if servers, ok := ck.config.Groups[gid]; ok {
			if _, ok = ck.leaderIds[gid]; !ok {
				ck.leaderIds[gid] = 0
			}
			oldLeaderId := ck.leaderIds[gid]
			newLeaderId := oldLeaderId
			ck.log("servers: %v", servers)
			for  {
				reply := OpCommandReply{}
				ck.log("try to send gid %d, server: %d", gid,newLeaderId)
				okk := ck.make_end(servers[newLeaderId]).Call("ShardKV.Operation", args, &reply)
				ck.log("reply: %+v", reply)
				if !okk {
					ck.log("call not ok")
				}
				if okk && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.leaderIds[gid] = newLeaderId
					return reply.Value
				} else if okk && reply.Err == ErrWrongGroup {
					break
				} else {
					newLeaderId = (newLeaderId + 1) % len(servers)
					if newLeaderId == oldLeaderId {
						break
					}
					continue
				}
			}
		}
		time.Sleep(RetryWait)
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) log(format string, a ...interface{}) {
	if ck.Debug {
		r := fmt.Sprintf(format, a...)
		log.Printf("client:%d, log: %s\n", ck.clientId, r)
	}
}