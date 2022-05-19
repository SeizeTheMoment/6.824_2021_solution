package shardkv

import (
	"6.824/labgob"
	"6.824/shardctrler"
	"bytes"
	"log"
)

func (kv *ShardKV) checkSnapshot(commandIndex int)  {
	if kv.maxraftstate == -1 || kv.persister.RaftStateSize() < kv.maxraftstate{
		return
	}
	data := kv.generateSnapshot()
	kv.rf.Snapshot(commandIndex, data)
}
func (kv *ShardKV) generateSnapshot() []byte  {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.stateMachines)
	e.Encode(kv.prevConfig)
	e.Encode(kv.currentConfig)
	//e.Encode(kv.lastAppliedIndex)
	data := w.Bytes()
	return data
}
func (kv *ShardKV) getPersistSnapshot()  {
	data := kv.persister.ReadSnapshot()
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var stateMachines map[int]*Shard
	var prevConfig shardctrler.Config
	var currentConfig shardctrler.Config
	//var lastAppliedIndex int
	if d.Decode(&stateMachines) != nil ||
		d.Decode(&prevConfig) != nil ||
		d.Decode(&currentConfig) != nil{
		log.Fatalln("read persist snapshot wrong!")
	} else {
		kv.stateMachines = stateMachines
		kv.prevConfig = prevConfig
		kv.currentConfig = currentConfig
	}
}