package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

const (
	GET = "Get"
	PUT = "Put"
	APPEND = "Append"
)
const WaitCmdTimeout = 500 * time.Millisecond
const (
	ConfigurationDuration = 50 * time.Millisecond
	PullShardDuration = 50 * time.Millisecond
	EraseShardDuration = 50 * time.Millisecond
	EnterEmptyDuration = 50 * time.Millisecond
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Method       string
	Key          string
	Value	     string

	ClientId     int64
	RequestId    int64
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead    int32 // set by Kill()

	prevConfig 		shardctrler.Config
	currentConfig 	shardctrler.Config
	sc			    *shardctrler.Clerk
	persister *raft.Persister
	mapNotify   map[int] chan NotifyMsg
	// Your definitions here.
	Debug   bool

	stateMachines  map[int]*Shard                        // shardId -> Shard
}

func (kv *ShardKV) Operation(args *OpCommandArgs, reply *OpCommandReply)  {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Method:    args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	res := kv.waitCmd(op)
	if kv.rf.GetRole() != raft.LEADER {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = res.Err
	reply.Value = res.Value
}

func (kv *ShardKV) waitCmd(op Op) (res NotifyMsg) {
	kv.lock()
	shardId := key2shard(op.Key)
	if op.Method != GET && kv.requestIsDuplicate(op.ClientId, op.RequestId, shardId) {
		res.Err = kv.stateMachines[shardId].LastSession[op.ClientId].Err
		kv.unlock()
		return res
	}
	if !kv.shardMatches(shardId) {
		res.Err = ErrWrongGroup
		kv.unlock()
		return res
	}
	kv.unlock()
	index, term, isLeader := kv.rf.Start(newRaftLogCommand(Operation, op))
	if !isLeader {
		res.Err = ErrWrongLeader
		return res
	}
	kv.lock("waitCmd")
	ch := make(chan NotifyMsg, 1)
	kv.mapNotify[index] = ch
	kv.unlock("waitCmd")
	kv.log("in waitCmd, execute start, index: %d, term: %d, isLeader: %v", index, term, isLeader)
	t := time.NewTimer(WaitCmdTimeout)
	defer t.Stop()
	select {
	case opNotify := <-ch:
		kv.lock("waitCmd")
		res = opNotify
		close(ch)
		delete(kv.mapNotify, index)
		kv.unlock("waitCmd")
	case <- t.C:
		kv.log("cmd timeout, op clientId: %d, requestId: %d", op.ClientId, op.RequestId)
		kv.lock("waitCmd")
		close(ch)
		delete(kv.mapNotify, index)
		res.Err = ErrTimeout
		kv.unlock("waitCmd")
	}
	return res
}
// 向另外的节点拉取shard
func (kv *ShardKV) tryPullShard()  {
	kv.lock("tryPullShard")
	oldGidToShards := kv.getOldGidToShards(Pulling)
	kv.log("pulling groupToshards: %+v", oldGidToShards)
	var wait sync.WaitGroup
	for gid, shardIds := range oldGidToShards {
		wait.Add(1)
		// 前任config的servers
		kv.log("in trypPullShard, gid: %d, shardId: %v", gid, shardIds)
		kv.log("prevConfig: %v", kv.prevConfig)
		go func(configNum int, servers []string, shardIds []int) {
			args := PullShardArgs{
				ShardIds:   shardIds,
				ConfigNum:  configNum,
			}
			kv.log("loop servers to send pull shards, servers: %v", servers)
			for _, server := range servers {
				reply := PullShardReply{}
				srv := kv.make_end(server)
				if srv.Call("ShardKV.PullShard", &args, &reply) && reply.Err == OK {
					kv.log("start insert command, %v", reply)
					kv.rf.Start(newRaftLogCommand(Insert, reply))
					break
				}
			}
			wait.Done()
		}(kv.currentConfig.Num, kv.prevConfig.Groups[gid], shardIds)

	}
	kv.unlock("tryPullShard")
	wait.Wait()
}
func (kv *ShardKV) PullShard(args *PullShardArgs, reply *PullShardReply)  {
	reply.Err = ErrWrongLeader
	// 只从leader中pull数据
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}
	kv.lock("pullShard")
	defer kv.unlock("pullShard")
	if args.ConfigNum > kv.currentConfig.Num {
		return
	}
	if kv.currentConfig.Num > args.ConfigNum {
		//panic("duplicated pull data request")
		return
	}
	replyShards := make(map[int]*Shard)
	for _, shardId := range args.ShardIds {
		shard := kv.stateMachines[shardId]
		replyShards[shardId] = shard.deepCopy()
	}
	reply.ConfigNum = kv.currentConfig.Num
	reply.Shards = replyShards
	reply.Err = OK
	return
}
/*
    EraseData
*/
// 先新config拉取，后通过旧config erase
func (kv *ShardKV) tryEraseShard()  {
	kv.lock()
	// 找到gid->shardId对应关系
	oldGidToShards := kv.getOldGidToShards(Waiting)
	kv.log("erasing groupToShards: %v", oldGidToShards)
	currentConfigNum := kv.currentConfig.Num
	wg := sync.WaitGroup{}
	for gid, shards := range oldGidToShards {
		wg.Add(1)
		servers := kv.prevConfig.Groups[gid]
		go func(configNum int, servers []string, shardsIds []int) {
			defer wg.Done()
			for _, server := range servers {
				srv := kv.make_end(server)
				args := EraseShardArgs{
					ConfigNum: configNum,
					ShardIds:  shardsIds,
				}
				reply := EraseShardReply{}
				if srv.Call("ShardKV.EraseShard", &args, &reply) && reply.Err == OK {
					kv.rf.Start(newRaftLogCommand(StopWaiting, args))
					break
				}
			}
		}(currentConfigNum, servers, shards)
	}
	kv.unlock()
	wg.Wait()
}
func (kv *ShardKV) EraseShard(args *EraseShardArgs, reply *EraseShardReply)  {
	index, _, isLeader := kv.rf.Start(newRaftLogCommand(Erase, *args))
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.log("start erase command: %v", *args)
	kv.lock()
	ch := make(chan NotifyMsg, 1)
	kv.mapNotify[index] = ch
	kv.unlock()
	t := time.NewTimer(WaitCmdTimeout)
	defer t.Stop()
	select {
	case res := <- ch:
		reply.Err = res.Err
	case <-t.C:
		reply.Err = ErrTimeout
	}
	go func() {
		kv.lock("deleteMapNotify")
		close(ch)
		delete(kv.mapNotify, index)
		kv.unlock("deleteMapNotify")
	}()
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}
func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

type NotifyMsg struct {
	Err      Err
	Value    string
	//ClientId int64
	//RequestId int64
}
func (kv* ShardKV) cmdApply()  {
	for !kv.killed(){
		select {
		case msg := <-kv.applyCh:

			if msg.CommandValid {
				kv.log("get commandMsg ready to apply: %v, msgIndex: %d", msg, msg.CommandIndex)
				command := msg.Command.(RaftLogCommand)
				kv.lock("cmdApply")
				res := NotifyMsg{}
				switch command.CommandType {
				case Operation:
					op := command.Data.(Op)
					res = kv.applyOperation(op)
					if notify, okk := kv.mapNotify[msg.CommandIndex]; okk {
						notify <- res
					}
					//kv.log("operation done, res: %v", res)
				case ConfigChange:
					latestConfig := command.Data.(shardctrler.Config)
					kv.applyConfigChange(&latestConfig)
					//kv.log("apply successfully, currentConfig: %v", kv.currentConfig)
				case Insert:
					pullShardReply := command.Data.(PullShardReply)
					kv.applyInsertShards(&pullShardReply)
					//kv.log("insert done")
				case StopWaiting:
					eraseShardArgs := command.Data.(EraseShardArgs)
					kv.applyStopWaiting(&eraseShardArgs)
					//kv.log("stopwaiting done")
				case Erase:
					eraseShardArgs := command.Data.(EraseShardArgs)
					res = kv.applyEraseShard(&eraseShardArgs)
					//kv.log("erase done, res: %v", res)
					if notify, okk := kv.mapNotify[msg.CommandIndex]; okk {
						notify <- res
					}
				case Empty:
				}
				kv.checkSnapshot(msg.CommandIndex)
				kv.unlock("cmdApply")
			} else {
				if msg.SnapshotValid {
					kv.lock("condInstall")
					if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
						kv.getPersistSnapshot()
					}
					kv.unlock("condInstall")
				}
			}
		}
	}
}


/*
	monitors
*/
func (kv *ShardKV) monitor(fun func(), timeout time.Duration)  {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			fun()
		}
		time.Sleep(timeout)
	}
}


// 每个Group的Leader需要在后台启动一个协程向shardCtrler定时使用Query拉取最新的Config，一旦拉取到就需要提交一条raft日志，已在每台机器上更新配置
// 每次只能拉取高一个版本的配置，而且为了防止集群的分片状态被覆盖，从而使得某些任务永远不会被执行，只有在每一Shard的状态都为Serving或Invalid时才能拉取、更新配置
func (kv *ShardKV) fetchConfiguration()  {
	fetchFlag := true
	kv.lock("fetchConfig")
	currentConfigNum := kv.currentConfig.Num
	// 必须等待至每一个Shard为Serving（可服务）或Invalid（不属于该组）时才可fetchConfig
	for _, shard := range kv.stateMachines {
		if shard.Status != Serving && shard.Status != Invalid {
			fetchFlag = false
			break
		}
	}
	kv.unlock("fetchConfig")
	if fetchFlag {
		latestConfig := kv.sc.Query(currentConfigNum + 1)
		if latestConfig.Num == currentConfigNum + 1 {
			kv.log("start configchange command %v", latestConfig)
			kv.rf.Start(newRaftLogCommand(ConfigChange, latestConfig))
		}
	}
}
func (kv *ShardKV) enterEmpty()  {
	if !kv.rf.HasLogAtCurrentTerm() {
		kv.log("start empty log")
		kv.rf.Start(newRaftLogCommand(Empty, nil))
	}
}
/*

*/

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(PullShardArgs{})
	labgob.Register(PullShardReply{})
	labgob.Register(EraseShardArgs{})
	labgob.Register(EraseShardReply{})
	labgob.Register(OpCommandArgs{})
	labgob.Register(RaftLogCommand{})
	labgob.Register(shardctrler.Config{})
	applyCh := make(chan raft.ApplyMsg)
	kv := &ShardKV{
		mu:                   sync.Mutex{},
		me:                   me,
		rf:                   raft.Make(servers, me, persister, applyCh),
		applyCh:              applyCh,
		make_end:             make_end,
		gid:                  gid,
		ctrlers:              ctrlers,
		maxraftstate:         maxraftstate,
		dead:                 0,
		prevConfig:           shardctrler.Config{Num: 0},
		currentConfig:        shardctrler.Config{Num: 0},
		sc:                   shardctrler.MakeClerk(ctrlers),
		persister:            persister,
		Debug:                false,
		stateMachines:        make(map[int]*Shard),
		mapNotify:            make(map[int]chan NotifyMsg),
	}
	kv.rf.DebugLog = false
	for shardId := 0; shardId < shardctrler.NShards; shardId++ {
		kv.stateMachines[shardId] = &Shard{
			KV:              make(map[string]string),
			Status:          Invalid,
			LastSession:     make(map[int64]*Session),
		}
	}
	kv.getPersistSnapshot()
	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)


	go kv.cmdApply()
	// 都是leader执行
	go kv.monitor(kv.fetchConfiguration, ConfigurationDuration)
	go kv.monitor(kv.tryPullShard, PullShardDuration)
	go kv.monitor(kv.tryEraseShard, EraseShardDuration)
	go kv.monitor(kv.enterEmpty, EnterEmptyDuration)
	return kv
}


/*
    Todo get ShardsByStatus
*/
func (kv *ShardKV) getOldGidToShards(status ShardStatus) map[int][]int{
	groupToShards := make(map[int][]int)
	for shardId := 0; shardId < shardctrler.NShards; shardId++ {
		if kv.stateMachines[shardId].Status == status {
			groupToShards[kv.prevConfig.Shards[shardId]] =
				append(groupToShards[kv.prevConfig.Shards[shardId]], shardId)
		}
	}
	return groupToShards
}


func (kv *ShardKV) log(format string, a ...interface{}) {
	////var status []ShardStatus
	////for i := 0; i < shardctrler.NShards; i++ {
	////	status = append(status, kv.stateMachines[i].Status)
	////}
	//if kv.Debug {
	//	r := fmt.Sprintf(format, a...)
	//	log.Printf("server: %d, gid: %d, raft role: %d, shards status: %+v, currentConfig: %+v, log:[%s]\n",
	//		kv.me, kv.gid, kv.rf.GetRole(), status, kv.currentConfig, r)
	//}
	if kv.Debug {
		r := fmt.Sprintf(format, a...)
		log.Printf("server: %d, gid: %d, raft role: %d,  currentConfig: %+v, log:[%s]\n",
			kv.me, kv.gid, kv.rf.GetRole(),kv.currentConfig, r)
	}
}

func (kv *ShardKV) lock(lockName... string)  {
	kv.mu.Lock()
	if kv.Debug {
		//kv.log("%s locked", lockName)
	}
}
func (kv *ShardKV) unlock(lockName ...string)  {
	if kv.Debug {
		//kv.log("%s unlocked", lockName)
	}
	kv.mu.Unlock()
}








