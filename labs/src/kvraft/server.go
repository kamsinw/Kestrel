package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type     string
	Key      string
	Value    string
	ClientId int64
	SeqNum   int64
}

type Result struct {
	Value string
	Err   Err
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	// Your definitions here.
	kvStore          map[string]string
	lastApplied      map[int64]int64
	waitChs          map[int]chan Result
	lastAppliedIndex int
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Type:     "Get",
		Key:      args.Key,
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
	}

	result := kv.executeOp(op)
	reply.Err = result.Err
	reply.Value = result.Value
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Type:     "Put",
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
	}

	result := kv.executeOp(op)
	reply.Err = result.Err
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Type:     "Append",
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
	}

	result := kv.executeOp(op)
	reply.Err = result.Err
}

func (kv *KVServer) executeOp(op Op) Result {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return Result{Err: ErrWrongLeader}
	}

	kv.mu.Lock()
	ch := make(chan Result, 1)
	kv.waitChs[index] = ch
	kv.mu.Unlock()

	select {
	case result := <-ch:
		return result
	case <-time.After(500 * time.Millisecond):
		kv.mu.Lock()
		delete(kv.waitChs, index)
		kv.mu.Unlock()
		return Result{Err: ErrWrongLeader}
	}
}

func (kv *KVServer) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvStore)
	e.Encode(kv.lastApplied)
	return w.Bytes()
}

func (kv *KVServer) decodeSnapshot(data []byte) {
	if len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvStore map[string]string
	var lastApplied map[int64]int64
	if d.Decode(&kvStore) != nil || d.Decode(&lastApplied) != nil {
		return
	}
	kv.kvStore = kvStore
	kv.lastApplied = lastApplied
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			kv.mu.Lock()
			if msg.CommandIndex <= kv.lastAppliedIndex {
				kv.mu.Unlock()
				continue
			}
			kv.lastAppliedIndex = msg.CommandIndex

			op := msg.Command.(Op)
			var result Result

			if op.Type != "Get" && kv.lastApplied[op.ClientId] >= op.SeqNum {
				result = Result{Err: OK}
			} else {
				switch op.Type {
				case "Get":
					value, ok := kv.kvStore[op.Key]
					if ok {
						result = Result{Value: value, Err: OK}
					} else {
						result = Result{Value: "", Err: ErrNoKey}
					}
				case "Put":
					kv.kvStore[op.Key] = op.Value
					result = Result{Err: OK}
					kv.lastApplied[op.ClientId] = op.SeqNum
				case "Append":
					kv.kvStore[op.Key] += op.Value
					result = Result{Err: OK}
					kv.lastApplied[op.ClientId] = op.SeqNum
				}
			}

			if ch, ok := kv.waitChs[msg.CommandIndex]; ok {
				ch <- result
				delete(kv.waitChs, msg.CommandIndex)
			}

			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
				snapshot := kv.encodeSnapshot()
				kv.rf.Snapshot(msg.CommandIndex, snapshot)
			}
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			kv.mu.Lock()
			if msg.SnapshotIndex > kv.lastAppliedIndex {
				kv.decodeSnapshot(msg.Snapshot)
				kv.lastAppliedIndex = msg.SnapshotIndex
			}
			kv.mu.Unlock()
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	// You may need initialization code here.
	kv.kvStore = make(map[string]string)
	kv.lastApplied = make(map[int64]int64)
	kv.waitChs = make(map[int]chan Result)

	kv.decodeSnapshot(persister.ReadSnapshot())

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.applier()

	return kv
}
