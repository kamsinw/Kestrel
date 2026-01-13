package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {

		args := GetTaskArgs{}
		reply := GetTaskReply{}

		ok := call("Coordinator.GetTask", &args, &reply)

		if !ok {
			return
		}

		switch reply.TaskType {
		case "map":
			filename := reply.Files[0]
			content, err := os.ReadFile(filename)
			if err != nil {
				log.Fatal("cant read", filename)
			}

			kva := mapf(filename, string(content))

			buckets := make([][]KeyValue, reply.NReduce)
			for _, kv := range kva {
				r := ihash(kv.Key) % reply.NReduce
				buckets[r] = append(buckets[r], kv)
			}

			for r := 0; r < reply.NReduce; r++ {
				oname := fmt.Sprintf("mr-%d-%d", reply.TaskID, r)
				ofile, _ := os.Create(oname)

				enc := json.NewEncoder(ofile)
				for _, kv := range buckets[r] {
					enc.Encode(&kv)
				}
				ofile.Close()

			}
			call("Coordinator.ReportTask",
				&ReportTaskArgs{TaskType: "map", TaskID: reply.TaskID},
				&ReportTaskReply{})

		case "reduce":
			intermediate := []KeyValue{}

			for _, filename := range reply.Files {
				file, err := os.Open(filename)
				if err != nil {
					continue
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				file.Close()
			}

			sort.Slice(intermediate, func(i, j int) bool {
				return intermediate[i].Key < intermediate[j].Key
			})
			oname := fmt.Sprintf("mr-out-%d", reply.TaskID)
			ofile, _ := os.Create(oname)

			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}

				output := reducef(intermediate[i].Key, values)
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			ofile.Close()
			call("Coordinator.ReportTask",
				&ReportTaskArgs{TaskType: "reduce", TaskID: reply.TaskID},
				&ReportTaskReply{})
		case "wait":
			time.Sleep(time.Second)
		case "exit":
			return
		}
	}

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
