package shardkv

import (
	"6.824/shardctrler"
)

// Operation functions
func (kv *ShardKV) applyOperation(op Op) NotifyMsg{
	res := NotifyMsg{}

	shardId := key2shard(op.Key)
	if kv.shardMatches(shardId) {
		kv.log("op: %v, kv: %v", op, kv.stateMachines[shardId].KV)
		if op.Method != GET && kv.requestIsDuplicate(op.ClientId, op.RequestId, shardId) {
			res.Err = kv.stateMachines[shardId].LastSession[op.ClientId].Err
			kv.log("encounter duplicate operation, res: %v", res)
			return res
		} else {
			res.Err, res.Value = kv.applyToStateMachines(op, shardId)
			kv.log("res.ERR: %v, res.Value: %v, kv: %v", res.Err, res.Value, kv.stateMachines[shardId].KV)
			if op.Method != GET {
				kv.stateMachines[shardId].LastSession[op.ClientId] = &Session{
					LastRequestedId: op.RequestId,
					Err: res.Err,
				}
				kv.log("update lastsession %v", kv.stateMachines[shardId].LastSession[op.ClientId])
			}
		}
		kv.log("op done kv: %v", kv.stateMachines[shardId].KV)
	} else {
		kv.log("config not ready!")
		res.Err = ErrWrongGroup
		res.Value = ""
	}
	return res
}
func (kv *ShardKV) shardMatches(shardId int) bool {
	kv.log("shardTogid:%v, gid:%v, status: %v", kv.currentConfig.Shards[shardId], kv.gid, kv.stateMachines[shardId].Status)
	return kv.currentConfig.Shards[shardId] == kv.gid &&
		(kv.stateMachines[shardId].Status == Serving || kv.stateMachines[shardId].Status == Waiting)
}
func (kv *ShardKV) requestIsDuplicate(clientId int64, requestId int64, shardId int) bool {

	lastSession, ok := kv.stateMachines[shardId].LastSession[clientId]
	if ok {
		kv.log("requestIsDuplicate func: lastRequestId: %d requestId: %d", lastSession.LastRequestedId, requestId)
	} else {
		kv.log("requestIsDuplicate func: lastRequestId: null, requestId: %d", requestId)
	}
	if !ok || requestId > lastSession.LastRequestedId {
		return false
	}
	return true
}
func (kv *ShardKV) applyToStateMachines(op Op, shardId int) (Err, string) {
	switch op.Method {
	case PUT:
		return kv.stateMachines[shardId].Put(op.Key, op.Value)
	case APPEND:
		return kv.stateMachines[shardId].Append(op.Key, op.Value)
	case GET:
		return kv.stateMachines[shardId].Get(op.Key)
	}
	return ErrWrongOp, ""
}
/*
    ConfigChange functions
*/
func (kv *ShardKV) applyConfigChange(nextConfig * shardctrler.Config)  {
	if nextConfig.Num == kv.currentConfig.Num + 1 {
		kv.updateShardStatus(nextConfig)
		kv.prevConfig = kv.currentConfig
		kv.currentConfig = *nextConfig
		kv.log("update config from %v to %v", kv.prevConfig, kv.currentConfig)
	}
}
func (kv *ShardKV) updateShardStatus(nextConfig *shardctrler.Config)  {

	// 若拉取到的是初始config
	if nextConfig.Num == 1 {
		for shardId, gid := range nextConfig.Shards {
			if kv.gid == gid {
				kv.stateMachines[shardId].Status = Serving
			}
		}
		kv.log("fetch first config successfully")
		return
	}

	// 找到新Config中新增的
	for shardId := 0; shardId < shardctrler.NShards; shardId++ {
		if nextConfig.Shards[shardId] == kv.gid && kv.currentConfig.Shards[shardId] != kv.gid {
			kv.stateMachines[shardId].Status = Pulling
		}
	}
	// 找到新Config中不在拥有的
	for shardId := 0; shardId < shardctrler.NShards; shardId++ {
		if nextConfig.Shards[shardId] != kv.gid && kv.currentConfig.Shards[shardId] == kv.gid {
			kv.stateMachines[shardId].Status = Erasing
		}
	}
}
/*
	Insert functions
*/
func (kv *ShardKV) applyInsertShards(shardsInfo *PullShardReply){
	// 比较configNum，相同才可执行
	if shardsInfo.ConfigNum == kv.currentConfig.Num {
		for shardId, shard := range shardsInfo.Shards {
			nowShard := kv.stateMachines[shardId]
			if nowShard.Status == Pulling {
				for key, value := range shard.KV {
					nowShard.KV[key] = value
				}
				for cid, session := range shard.LastSession {
					nowShard.LastSession[cid] = session
				}
				kv.stateMachines[shardId].Status = Waiting
			} else {
				break
			}
		}
	}
}


/*
	StopWaiting functions
*/
func (kv *ShardKV) applyStopWaiting(args *EraseShardArgs)  {
	if args.ConfigNum == kv.currentConfig.Num {
		for _, shardId := range args.ShardIds {
			shard := kv.stateMachines[shardId]
			if shard.Status == Waiting {
				shard.Status = Serving
			} else {
				break
			}
		}
	}
}
/*
	Erase functions
*/

func (kv *ShardKV) applyEraseShard(args *EraseShardArgs) (res NotifyMsg) {
	if args.ConfigNum == kv.currentConfig.Num {
		for _, shardId := range args.ShardIds {
			shard := kv.stateMachines[shardId]
			if shard.Status == Erasing {
				// 删除
				kv.stateMachines[shardId] = &Shard{
					KV:              make(map[string]string),
					Status:          Invalid,
					LastSession:     make(map[int64]*Session),
				}
				kv.log("applyErasedShard..., shard erased")
			} else {
				break
			}
		}
		res.Err = OK
		return res
	}
	res.Err = OK
	return res
}
