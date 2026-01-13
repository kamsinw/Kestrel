package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

const (
	TaskTypeMap    = "map"
	TaskTypeReduce = "reduce"
	TaskTypeWait   = "wait"
	TaskTypeDone   = "done"
)

type GetTaskArgs struct {
	WorkerID int
}

type GetTaskReply struct {
	TaskType string
	TaskID   int
	Files    []string
	NReduce  int
	NMap     int
	Done     bool
}

type ReportTaskArgs struct {
	TaskType string
	TaskID   int
	WorkerID int
}
type ReportTaskReply struct {
	Ack bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
