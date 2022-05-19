package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0
const WaitCmdTimeout = 500 * time.Millisecond
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
func (kv *KVServer) log(format string, a ...interface{}) {
	if kv.Debug {
		r := fmt.Sprintf(format, a...)
		log.Printf("server: %d, raft role: %d, log:[%s]\n",
			kv.me, kv.rf.GetRole(), r)
	}
}
type NotifyMsg struct {
	Err      Err
	Value    string
}
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

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	persister *raft.Persister
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	Debug   bool
	db      map[string] string
	//* mapCh key: requestId value: Op with clientId, avoiding multiple clients operate the same server
	//* if
	mapNotify   map[int] chan NotifyMsg
	//* lastAppliedRequestId key: clientId value: requestId that last applied
	lastAppliedRequestId map[int64] int64

}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		Method:    GET,
		Key:       args.Key,
		Value:     "",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	// kv.log("server get: key: %v", args.Key)
	res := kv.waitCmd(op)
	if kv.rf.GetRole() != raft.LEADER {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = res.Err
	reply.Value = res.Value
	kv.log("get done, key: %v, value: %v", args.Key, reply.Value)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
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
	kv.log("server put append key: %v, op: %v, value: %v", args.Key, args.Op,args.Value)
	res := kv.waitCmd(op)
	if kv.rf.GetRole() != raft.LEADER {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = res.Err
}
func (kv *KVServer) getValue(key string) (res NotifyMsg) {
	value, ok := kv.db[key]
	if ok {
		res.Err = OK
		res.Value = value
	} else {
		res.Err = ErrNoKey
	}
	return res
}
func (kv *KVServer) waitCmd(op Op) (res NotifyMsg) {
	//kv.log("waitcmd, op: %v", op)
	//kv.mu.Lock()
	//kv.log("waitcmd locked")
	//lastRequestId, ok := kv.lastAppliedRequestId[op.ClientId]
	//if op.Method != GET && (ok && op.RequestId == lastRequestId) {
	//	res.Err = OK
	//	kv.mu.Unlock()
	//	return res
	//}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		res.Err = ErrWrongLeader
		return res
	}
	kv.mu.Lock()
	ch := make(chan NotifyMsg, 1)
	kv.mapNotify[index] = ch
	kv.mu.Unlock()
	kv.log("in waitCmd, execute start, index: %d, term: %d, isLeader: %v", index, term, isLeader)
	t := time.NewTimer(WaitCmdTimeout)
	defer t.Stop()
	select {
	case opNotify := <-ch:
		kv.mu.Lock()
		res = opNotify
		close(ch)
		delete(kv.mapNotify, index)
		kv.mu.Unlock()
	case <-t.C:
		kv.log("cmd timeout, op clientId: %d, requestId: %d", op.ClientId, op.RequestId)
		kv.mu.Lock()
		close(ch)
		delete(kv.mapNotify, index)
		res.Err = ErrTimeout
		kv.mu.Unlock()
	}
	return res
}
func (kv* KVServer) cmdApply()  {
	for !kv.killed(){
		select {
		case ch := <-kv.applyCh:
			if ch.CommandValid == false {
				if ch.SnapshotValid == true {
					kv.mu.Lock()
					flag := kv.rf.CondInstallSnapshot(ch.SnapshotTerm, ch.SnapshotIndex, ch.Snapshot)
					if flag {
						kv.getPersistSnapshot()
					}
					kv.mu.Unlock()
				}
				continue
			}
			kv.log("get msg: %v, msgIndex: %d", ch, ch.CommandIndex)
			op := ch.Command.(Op)
			kv.mu.Lock()
			lastAppliedId, ok := kv.lastAppliedRequestId[op.ClientId]
			//kv.log("judge requestId and lastAppliedId, %d %d", op.RequestId,lastAppliedId)
			if !ok || op.RequestId != lastAppliedId {
				switch op.Method {
				case PUT:
					kv.db[op.Key] = op.Value
					kv.log("put done. key: %v, value: %v, clientId: %d, requestId: %d", op.Key, kv.db[op.Key],
						op.ClientId, op.RequestId)
					kv.lastAppliedRequestId[op.ClientId] = op.RequestId
				case APPEND:
					kv.db[op.Key] += op.Value
					kv.log("append done. key: %v, op.value: %v, append done value: %v, clientId: %d, requestId: %d",
						op.Key, op.Value,kv.db[op.Key],
						op.ClientId, op.RequestId)
					kv.lastAppliedRequestId[op.ClientId] = op.RequestId
				case GET:
				default:
					panic("wrong method")
				}
			}
			notify, ok := kv.mapNotify[ch.CommandIndex]
			res := kv.getValue(op.Key)
			kv.mu.Unlock()
			if ok {
				notify <- NotifyMsg{
					Err:   res.Err,
					Value: res.Value,
				}
			}
			kv.mu.Lock()
			kv.checkSnapshot(ch.CommandIndex)
			kv.mu.Unlock()
		}
	}
}
func (kv *KVServer) checkSnapshot(commandIndex int)  {
	if kv.maxraftstate == -1 || kv.persister.RaftStateSize() < kv.maxraftstate{
		return
	}
	data := kv.generateSnapshot()
	kv.rf.Snapshot(commandIndex, data)
}
func (kv *KVServer) generateSnapshot() []byte  {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.db)
	e.Encode(kv.lastAppliedRequestId)
	data := w.Bytes()
	return data
}
func (kv *KVServer) getPersistSnapshot()  {
	data := kv.persister.ReadSnapshot()
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var db map[string]string
	var lastAppliedRequestId map[int64]int64
	if d.Decode(&db) != nil ||
		d.Decode(&lastAppliedRequestId) != nil {
		log.Fatalln("read persist snapshot wrong!")
	} else {
		kv.db = db
		kv.lastAppliedRequestId = lastAppliedRequestId
	}
}
//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.log("ready to kill....")
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	// You may need initialization code here.
	kv.dead = 0
	kv.mu = sync.Mutex{}
	kv.Debug = false
	kv.db = make(map[string]string)
	kv.lastAppliedRequestId = make(map[int64]int64)
	kv.mapNotify = make(map[int]chan NotifyMsg)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.persister = persister
	kv.getPersistSnapshot()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// You may need initialization code here.
	kv.log("start...")
	go kv.cmdApply()
	go kv.enterEmpty()
	return kv
}
func (kv *KVServer) enterEmpty()  {
	if !kv.rf.HasLogAtCurrentTerm() {
		kv.log("start empty log")
		kv.rf.Start(raft.ApplyMsg{
			CommandValid:  false,
			Command:       nil,
			CommandIndex:  0,
			SnapshotValid: false,
			Snapshot:      nil,
			SnapshotTerm:  0,
			SnapshotIndex: 0,
		})
	}
}
func (kv *KVServer) getSnapshotDb() (m map[string]string) {
	data := kv.persister.ReadSnapshot()
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var lastAppliedRequestId map[int64]int64
	if d.Decode(&m) != nil ||
		d.Decode(&lastAppliedRequestId) != nil {
		log.Fatalln("read persist snapshot wrong!")
	} else {

	}
	return
}