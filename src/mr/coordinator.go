package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	IDLE = iota
	INPROGRESS
	COMPLETED
)

type Coordinator struct {
	// Your definitions here.
	nMap    int
	nReduce int

	mapTasks    map[int]string
	reduceTasks map[int]string

	mapFinished    bool
	reduceFinished bool

	mapStates    map[int]int
	reduceStates map[int]int

	mapWorkers    map[int]int
	reduceWorkers map[int]int

	mapStartTimes    map[int]time.Time
	reduceStartTimes map[int]time.Time

	mutex sync.Mutex
	cond  *sync.Cond
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) AssignTask(args *ExampleArgs, reply *TaskInfo) error {
	for {
		c.mutex.Lock()
		if !c.mapFinished {
			// update Map tasks' states
			// for i := range c.mapStates {
			// 	if c.mapStates[i] == INPROGRESS {
			// 		currentTime := time.Now()
			// 		if currentTime.Sub(c.mapStartTimes[i]).Seconds() > 10 {
			// 			c.mapStates[i] = IDLE
			// 			c.cond.Broadcast()
			// 		}
			// 	}
			// }

			// search for an unstarted map task
			for i := range c.mapStates {
				if c.mapStates[i] == IDLE {
					reply.Filename = c.mapTasks[i] // reply with this file
					reply.TaskNumber = i           // set task number as i
					reply.TaskType = MAP           // set task type as MAP
					reply.NReduce = c.nReduce      // pass nReduce
					reply.NMap = c.nMap            // pass nMap
					c.mapStates[i] = INPROGRESS    // mark the task as started
					c.mapStartTimes[i] = time.Now()
					c.mutex.Unlock()
					return nil
				}
			}

			// all map tasks are in process
			c.cond.Wait()
			c.mutex.Unlock()
		} else if !c.reduceFinished {
			// update Reduce tasks' states
			// for i := range c.reduceStates {
			// 	if c.reduceStates[i] == INPROGRESS {
			// 		currentTime := time.Now()
			// 		if currentTime.Sub(c.reduceStartTimes[i]).Seconds() > 10 {
			// 			c.reduceStates[i] = IDLE
			// 			c.cond.Broadcast()
			// 		}
			// 	}
			// }

			// search for an unstarted reduce task
			for i := range c.reduceStates {
				if c.reduceStates[i] == IDLE {
					reply.TaskNumber = i           // set task number as i
					reply.TaskType = REDUCE        // set task type as REDUCE
					reply.NReduce = c.nReduce      // pass nReduce
					reply.NMap = c.nMap            // pass nMap
					c.reduceStates[i] = INPROGRESS // mark the task as started
					c.reduceStartTimes[i] = time.Now()
					c.mutex.Unlock()
					return nil
				}
			}

			// all reduce tasks are in process
			c.cond.Wait()
			c.mutex.Unlock()
		} else {
			reply.TaskType = NOTASK
			c.mutex.Unlock()
			return nil
		}
	}
}

func (c *Coordinator) AcknowledgeCompletion(task *TaskInfo, reply *ExampleReply) error {
	c.mutex.Lock()
	if task.TaskType == MAP {
		c.mapStates[task.TaskNumber] = COMPLETED
		for _, state := range c.mapStates {
			if state == IDLE || state == INPROGRESS {
				c.mutex.Unlock()
				return nil
			}
		}
		c.mapFinished = true
		c.cond.Broadcast()
	} else {
		c.reduceStates[task.TaskNumber] = COMPLETED
		for _, state := range c.reduceStates {
			if state == IDLE || state == INPROGRESS {
				c.mutex.Unlock()
				return nil
			}
		}
		c.reduceFinished = true
		c.cond.Broadcast()
	}
	c.mutex.Unlock()
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

	// Your code here.
	if !c.mapFinished {
		c.mutex.Lock()
		for i := range c.mapStates {
			if c.mapStates[i] == INPROGRESS {
				currentTime := time.Now()
				if currentTime.Sub(c.mapStartTimes[i]).Seconds() > 10 {
					c.mapStates[i] = IDLE
					c.cond.Broadcast()
				}
			}
		}
		c.mutex.Unlock()
	} else if !c.reduceFinished {
		c.mutex.Lock()
		for i := range c.reduceStates {
			if c.reduceStates[i] == INPROGRESS {
				currentTime := time.Now()
				if currentTime.Sub(c.reduceStartTimes[i]).Seconds() > 10 {
					c.reduceStates[i] = IDLE
					c.cond.Broadcast()
				}
			}
		}
		c.mutex.Unlock()
	} else {
		ret = true
		os.RemoveAll("mr-tmp/")
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	// Your code here.
	tmpPath := filepath.Join("mr-tmp")
	os.MkdirAll(tmpPath, os.ModePerm)

	c := Coordinator{
		len(files), nReduce,
		make(map[int]string), make(map[int]string),
		false, false,
		make(map[int]int), make(map[int]int),
		make(map[int]int), make(map[int]int),
		make(map[int]time.Time), make(map[int]time.Time),
		sync.Mutex{}, nil,
	}

	c.cond = sync.NewCond(&c.mutex)

	for i, filename := range files {
		c.mapTasks[i] = filename
		c.mapStates[i] = IDLE
	}

	for i := 0; i < nReduce; i++ {
		c.reduceStates[i] = IDLE
	}

	c.server()
	return &c
}
