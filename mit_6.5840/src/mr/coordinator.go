package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	MapTask    = "map"
	ReduceTask = "reduce"
	WaitTask   = "wait"
	ExitTask   = "exit"
)

type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Completed
)

type Task struct {
	Id        int
	State     TaskState
	StartTime time.Time
	File      string
}

type Coordinator struct {
	// Your definitions here.
	mu          sync.Mutex
	phase       string
	mapTasks    []Task
	reduceTasks []Task

	nReduce int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.phase {

	case MapTask:
		for i := range c.mapTasks {
			task := &c.mapTasks[i]
			if task.State == Idle || (task.State == InProgress && time.Since(task.StartTime) > 10*time.Second) {
				task.State = InProgress
				task.StartTime = time.Now()

				reply.TaskType = MapTask
				reply.TaskID = task.Id
				reply.Files = []string{task.File}
				reply.NReduce = c.nReduce
				return nil
			}
		}

		if c.allMapDone() {
			c.phase = ReduceTask
		}

		reply.TaskType = WaitTask
		return nil

	case ReduceTask:
		for i := range c.reduceTasks {
			task := &c.reduceTasks[i]
			if task.State == Idle || (task.State == InProgress && time.Since(task.StartTime) > 10*time.Second) {
				task.State = InProgress
				task.StartTime = time.Now()

				reply.TaskType = ReduceTask
				reply.TaskID = task.Id

				files := []string{}
				for _, m := range c.mapTasks {
					files = append(files, makeReduceFilename(m.Id, task.Id))
				}
				reply.Files = files
				return nil
			}
		}

		if c.allReduceDone() {
			c.phase = ExitTask
		}

		reply.TaskType = WaitTask
		return nil

	default:
		reply.TaskType = ExitTask
		return nil
	}
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.TaskType {
	case MapTask:
		if c.mapTasks[args.TaskID].State == InProgress {
			c.mapTasks[args.TaskID].State = Completed
		}

	case ReduceTask:
		if c.reduceTasks[args.TaskID].State == InProgress {
			c.reduceTasks[args.TaskID].State = Completed
		}
	}

	reply.Ack = true
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	c.mu.Lock()
	defer c.mu.Unlock()
	ret = c.phase == ExitTask
	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		phase:   MapTask,
		nReduce: nReduce,
	}

	// Your code here.
	for i, f := range files {
		c.mapTasks = append(c.mapTasks, Task{
			Id:    i,
			State: Idle,
			File:  f,
		})
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks = append(c.reduceTasks, Task{
			Id:    i,
			State: Idle,
		})
	}

	c.server()
	return &c
}

// ================= HELPERS =================

func (c *Coordinator) allMapDone() bool {
	for _, t := range c.mapTasks {
		if t.State != Completed {
			return false
		}
	}
	return true
}

func (c *Coordinator) allReduceDone() bool {
	for _, t := range c.reduceTasks {
		if t.State != Completed {
			return false
		}
	}
	return true
}

func makeReduceFilename(mapId, reduceId int) string {
	return "mr-" + itoa(mapId) + "-" + itoa(reduceId)
}

func itoa(x int) string {
	return fmt.Sprintf("%d", x)
}
