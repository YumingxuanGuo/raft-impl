package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	// Your worker implementation here.

	for {
		// ask the coordinator for a task
		task := AskForTask()

		switch task.TaskType {
		case MAP:
			// call Map function
			PerformMap(task, mapf)

		case REDUCE:
			// call Reduce function
			PerformReduce(task, reducef)

		case NOTASK:
			return
		}

		// signal the coordinator that task is finished
		SignalCompletion(task)
	}
}

func AskForTask() *TaskInfo {
	// declare a reply structure.
	args := ExampleArgs{}
	reply := TaskInfo{}

	ok := call("Coordinator.AssignTask", &args, &reply)
	if !ok {
		fmt.Printf("AssignTask call failed!\n")
	}

	return &reply
}

func SignalCompletion(task *TaskInfo) {
	reply := ExampleReply{}
	ok := call("Coordinator.AcknowledgeCompletion", task, &reply)
	if !ok {
		fmt.Printf("AcknowledgeCompletion call failed!\n")
	}
}

func PerformMap(task *TaskInfo, mapf func(string, string) []KeyValue) {
	file, err := os.Open(task.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", task.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.Filename)
	}
	file.Close()
	kvs := mapf(task.Filename, string(content))

	intermediateFiles := make([]*os.File, task.NReduce)
	encoders := make([]*json.Encoder, task.NReduce)
	for i := 0; i < task.NReduce; i++ {
		intermediateFiles[i], _ = ioutil.TempFile("mr-tmp", "mr-tmp-*")
		encoders[i] = json.NewEncoder(intermediateFiles[i])
	}

	for _, kv := range kvs {
		reduceTaskNumber := ihash(kv.Key) % task.NReduce
		err := encoders[reduceTaskNumber].Encode(&kv)
		if err != nil {
			log.Fatalf("file [%v]: cannot write entry <%v, %v> to %v, reduceTaskNumber: %d",
				task.Filename, kv.Key, kv.Value, "[?]", reduceTaskNumber)
			panic("Json failed")
		}
	}

	for i, file := range intermediateFiles {
		intermediateFilename := fmt.Sprintf("mr-tmp/mr-%d-%d", task.TaskNumber, i)
		oldPath := fmt.Sprintf("%v", intermediateFiles[i].Name())
		err := os.Rename(oldPath, intermediateFilename)
		if err != nil {
			log.Fatalf("cannot rename file [%v]", oldPath)
			panic("Rename failed")
		}
		file.Close()
	}
}

func PerformReduce(task *TaskInfo, reducef func(string, []string) string) {
	// open files with format "mr-*-[reduceTaskNumber]"
	intermediateFiles := make([]*os.File, task.NMap)
	decoders := make([]*json.Decoder, task.NMap)
	for i := 0; i < task.NMap; i++ {
		intermediateFilename := fmt.Sprintf("mr-tmp/mr-%d-%d", i, task.TaskNumber)
		var err error
		intermediateFiles[i], err = os.Open(intermediateFilename)
		if err != nil {
			log.Fatalf("cannot open file [%v]", intermediateFilename)
			panic("Open failed")
		}
		decoders[i] = json.NewDecoder(intermediateFiles[i])
	}

	kvs := make([]KeyValue, 0)
	for _, dec := range decoders {
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
	}

	sort.Sort(ByKey(kvs))

	filename := fmt.Sprintf("mr-out-%d", task.TaskNumber)
	file, _ := os.Create(filename)

	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[i].Key, values)
		fmt.Fprintf(file, "%v %v\n", kvs[i].Key, output)
		i = j
	}

	file.Close()
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
