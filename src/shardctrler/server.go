package shardctrler

import (
	"6.824/raft"
	"fmt"
	"log"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

const WaitCmdTimeout = 500 * time.Millisecond
type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	// Your data here.
	configs []Config // indexed by config num
	lastAppliedRequestId map[int64] int64
	mapOpNotify          map[int] chan Op
	Debug                bool
}

type Reply struct {
	Err Err
	WrongLeader bool
	Config      Config
}
type Op struct {
	OpType       string
	ClientId     int64
	RequestId    int64
	OpArgs       interface{}
}

func (sc  *ShardCtrler) log(format string, a ...interface{})  {
	if sc.Debug {
		r := fmt.Sprintf(format, a...)
		log.Printf("server: %d, raft role: %d, config:%+v, log:[%s]\n",
			sc.me, sc.rf.GetRole(), sc.configs,r)
	}
}
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	cmd := Op{
		OpType:    JOIN,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		OpArgs:  *args,
	}
	res := sc.waitCmd(cmd)
	reply.Err = res.Err
	reply.WrongLeader = res.WrongLeader
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	cmd := Op{
		OpType:    LEAVE,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		OpArgs:  *args,
	}
	res := sc.waitCmd(cmd)
	reply.Err = res.Err
	reply.WrongLeader = res.WrongLeader
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	cmd := Op{
		OpType:    MOVE,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		OpArgs:    *args,
	}
	res := sc.waitCmd(cmd)
	reply.Err = res.Err
	reply.WrongLeader = res.WrongLeader
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	cmd := Op{
		OpType:    QUERY,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		OpArgs:    *args,
	}
	res := sc.waitCmd(cmd)
	reply.Err = res.Err
	reply.WrongLeader = res.WrongLeader
	reply.Config = res.Config
}

func (sc *ShardCtrler) waitCmd(op Op) (reply Reply) {
	sc.log("enter wait cmd, op: %+v", op)
	index, _, isLeader := sc.rf.Start(op)
	if isLeader == false {
		reply.WrongLeader = true
		return
	}
	sc.lock("waitCmd")
	ch := make(chan Op, 1)
	sc.mapOpNotify[index] = ch
	sc.unlock("waitCmd")
	t := time.NewTimer(WaitCmdTimeout)
	defer t.Stop()
	select {
	case opNotify := <-ch:
		sc.lock("waitCmd2")
		if opNotify.ClientId == op.ClientId && opNotify.RequestId == op.RequestId {
			switch opNotify.OpType {
			case JOIN:
				reply.Err = OK
			case LEAVE:
				reply.Err = OK
			case MOVE:
				reply.Err = OK
			case QUERY:
				sc.log("query execute..")
				queryArgs := op.OpArgs.(QueryArgs)
				reply.Err = OK
				if queryArgs.Num >= 0 && queryArgs.Num < len(sc.configs) {
					reply.Config = sc.configs[queryArgs.Num]
				} else {
					reply.Config = sc.configs[len(sc.configs) - 1]
				}

			}
		} else {
			reply.WrongLeader = true
		}
		close(ch)
		delete(sc.mapOpNotify, index)
		sc.log("delete mapNotify: %d", index)
		sc.unlock("waitCmd2")
	case <-t.C:
		sc.lock("waitCmdT")
		reply.WrongLeader = true
		close(ch)
		delete(sc.mapOpNotify, index)
		sc.unlock("waitCmdT")
	}
	return
}
//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}
func (sc *ShardCtrler) cmdApply()  {
	for {
		select {
		case ch := <- sc.applyCh:
			if ch.CommandValid == true {
				op := ch.Command.(Op)
				sc.lock("cmdApply")
				lastApplied, ok := sc.lastAppliedRequestId[op.ClientId]
				if !ok || op.RequestId > lastApplied {
					switch op.OpType{
					case JOIN:
						joinArgs := op.OpArgs.(JoinArgs)
						sc.log("join args: %+v", joinArgs)
						lastConfig := sc.configs[len(sc.configs) - 1]
						newGroups := getDeepCopy(lastConfig.Groups)
						for gid, servers := range joinArgs.Servers {
							newGroups[gid] = servers
						}
						newConfig := Config{
							Num:    lastConfig.Num + 1,
							Shards: lastConfig.Shards,
							Groups: newGroups,
						}
						var unusedShards []int
						for shardId, gid := range newConfig.Shards {
							if gid == 0 {
								unusedShards = append(unusedShards, shardId)
							}
						}
						groupToShards := getGroupToShards(&newConfig)
						// 将未分配的shard分配至拥有最少shard的group中
						for _, shardId := range unusedShards {
							miniGid, _ := getMinGroupId(groupToShards)
							groupToShards[miniGid] = append(groupToShards[miniGid], shardId)
							newConfig.Shards[shardId] = miniGid
						}
						rebalanced(&newConfig)
						sc.configs = append(sc.configs, newConfig)
						sc.log("join done, op: %+v, newConfig: %+v", joinArgs, newConfig)
					case LEAVE:
						leaveArgs := op.OpArgs.(LeaveArgs)
						sc.log("leave args: %+v", leaveArgs)
						lastConfig := sc.configs[len(sc.configs) - 1]
						newGroups := getDeepCopy(lastConfig.Groups)
						newConfig := Config{
							Num:    lastConfig.Num + 1,
							Shards: lastConfig.Shards,
							Groups: newGroups,
						}
						var unusedShards []int
						groupToShards := getGroupToShards(&newConfig)
						for _, gid := range leaveArgs.GIDs {
							delete(newConfig.Groups, gid)
							if shards, okk := groupToShards[gid]; okk {
								unusedShards = append(unusedShards, shards...)
								delete(groupToShards, gid)
							}
						}
						// 将新的unused的shard分配给拥有最少shard的group
						for _, shardId := range unusedShards {
							miniGid, _ := getMinGroupId(groupToShards)
							groupToShards[miniGid] = append(groupToShards[miniGid], shardId)
							newConfig.Shards[shardId] = miniGid
						}
						rebalanced(&newConfig)
						sc.configs = append(sc.configs, newConfig)
					case MOVE:
						lastConfig := sc.configs[len(sc.configs) - 1]
						newGroups := getDeepCopy(lastConfig.Groups)
						newConfig := Config{
							Num:    lastConfig.Num + 1,
							Shards: lastConfig.Shards,
							Groups: newGroups,
						}
						moveArgs := op.OpArgs.(MoveArgs)
						if _, isExists := newConfig.Groups[moveArgs.GID]; isExists {
							newConfig.Shards[moveArgs.Shard] = moveArgs.GID
						} else {
							return
						}
						sc.configs = append(sc.configs, newConfig)
					case QUERY:

					}
					sc.lastAppliedRequestId[op.ClientId] = lastApplied
				}
				sc.log("mapNotify ready to enter: %d", ch.CommandIndex)
				if notify, okk := sc.mapOpNotify[ch.CommandIndex]; okk {
					notify <- op
				}
				sc.log("notify entered")
				sc.unlock("cmdApply")
			}

		}
	}
}
func getDeepCopy(groups map[int][]string) map[int][]string{
	newGroups := make(map[int][]string)
	for gid, servers := range groups {
		newGroups[gid] = servers
	}
	return newGroups
}
func rebalanced(newConfig *Config)  {
	groupToShards := getGroupToShards(newConfig)
	var mini, miniGid int
	var maxi, maxiGid int
	for true{
		miniGid, mini = getMinGroupId(groupToShards)
		maxiGid, maxi = getMaxGroupId(groupToShards)
		if maxi - mini <= 1 {
			break
		}
		pos := groupToShards[maxiGid][0]
		newConfig.Shards[pos] = miniGid
		groupToShards[maxiGid] = groupToShards[maxiGid][1:]
		groupToShards[miniGid] = append(groupToShards[miniGid], pos)
	}
}
//func newRebalanced(newConfig *Config) {
//	groupToShards := getGroupToShards(newConfig)
//	avg := NShards / len(newConfig.Groups)
//
//}
func getGroupToShards(config *Config) map[int][]int{
	// gid -> shards
	groupToShards := make(map[int][]int)
	for gid, _ := range config.Groups {
		groupToShards[gid] = []int{}
	}
	for shardId, gid := range config.Shards {
		groupToShards[gid] = append(groupToShards[gid], shardId)
	}
	return groupToShards
}
func getMinGroupId(shardsMap map[int][]int) (miniGid int, mini int){
	mini = NShards + 1
 	for gid, shards := range shardsMap {
		if len(shards) < mini {
			miniGid = gid
			mini = len(shards)
		} else if len(shards) == mini {
			if gid < miniGid {
				miniGid = gid
			}
		}
	}
	return miniGid, mini
}
func getMaxGroupId(shardsMap map[int][]int) (maxGid int, maxi int){
	maxi = 0
	for gid, shards := range shardsMap {
		if len(shards) > maxi {
			maxGid = gid
			maxi = len(shards)
		} else if len(shards) == maxi {
			if gid < maxGid {
				maxGid = gid
			}
		}
	}
	return maxGid, maxi
}
//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	sc.configs[0].Shards = [NShards]int{}
	sc.configs[0].Num = 0

	sc.mapOpNotify = make(map[int]chan Op)
	sc.lastAppliedRequestId = make(map[int64]int64)

	sc.Debug = false

	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	go sc.cmdApply()
	return sc
}
func (sc *ShardCtrler) lock(lockName ... string)  {
	sc.mu.Lock()
	sc.log("%s locked", lockName)

}
func (sc *ShardCtrler) unlock(lockName ... string)  {
	sc.log("%s unlocked", lockName)
	sc.mu.Unlock()
}