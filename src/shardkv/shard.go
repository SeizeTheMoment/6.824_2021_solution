package shardkv
type ShardStatus uint8

const (
	// 服务状态
	Serving ShardStatus = iota
	// 等待拉取
	Pulling
	// 该group不拥有该shard
	Invalid
	// 该group拥有该shard但是不服务
	Erasing
	// 等待前一个owner删除它
	Waiting
)

type Session struct {
	LastRequestedId    int64
	Err                Err
	//Result             NotifyMsg
}
type Shard struct {
	KV	   map[string]string
	Status ShardStatus
	//LastRequestedId map[int64]int64
	LastSession     map[int64]*Session
}
func (shard *Shard) Get(key string) (Err Err,value string) {
	if val, ok := shard.KV[key]; ok {
		return OK, val
	}
	return ErrNoKey, ""
}

func (shard *Shard) Put(key string, value string) (Err, string) {
	shard.KV[key] = value
	return OK, value
}
func (shard *Shard) Append(key string, value string) (Err, string) {
	shard.KV[key] += value
	return OK, shard.KV[key]
}
func (shard *Shard) deepCopy() *Shard {
	newShard := Shard{
		KV:              make(map[string]string),
		Status:          Serving,
		LastSession:     map[int64]*Session{},
	}
	for cid, session := range shard.LastSession {
		 newShard.LastSession[cid] = session
	}
	for k, v := range shard.KV {
		newShard.KV[k] = v
	}
	return &newShard
}