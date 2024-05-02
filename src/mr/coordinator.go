package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// TODO: Handout reduce tasks along with map tasks to do the computing early on and
// give out responsibility to workers as early as possible

type MapTask struct {
	TaskID       string
	BucketNumber int
	InputFile    string
}

type ReduceTask struct {
	TaskID          string
	BucketNumber    int
	InputFilePrefix string
}

type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex

	nReduce int
	mMap    int
	// size: N*M

	// to track files that are processes by map function
	mapTasks map[string]struct{}

	// to track bucket numbers that are processed by reduce function
	reduceTasks map[int]struct{}

	// This timer waits for completion
	mapTaskCompletionTimer    map[string]chan struct{}
	reduceTaskCompletionTimer map[int]chan struct{}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) createMapTasks(sourceFiles []string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	mapTasks := map[string]struct{}{}

	for _, f := range sourceFiles {
		mapTasks[f] = struct{}{}
		c.mapTaskCompletionTimer[f] = make(chan struct{})
	}

	c.mapTasks = mapTasks
}

func (c *Coordinator) createReduceTasks(nReduce int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	reduceTasks := map[int]struct{}{}

	for i := 0; i < nReduce; i++ {
		reduceTasks[i] = struct{}{}
		c.reduceTaskCompletionTimer[i] = make(chan struct{})
	}

	c.reduceTasks = reduceTasks
}

func (c *Coordinator) startMapTaskTimer(mapKey string) {
	go func() {
		timer := time.NewTimer(10 * time.Second)

		select {
		case <-timer.C:
			// Reinsert task
			fmt.Println("10 secs elapsed, map task not completed", mapKey)
			c.mu.Lock()
			defer c.mu.Unlock()

			c.mapTasks[mapKey] = struct{}{}
		case <-c.mapTaskCompletionTimer[mapKey]:
			// Task complete
		}

		timer.Stop()
	}()
}

func (c *Coordinator) startReduceTaskTimer(reduceKey int) {
	go func() {
		timer := time.NewTimer(10 * time.Second)

		select {
		case <-timer.C:
			// Reinsert task
			c.mu.Lock()
			defer c.mu.Unlock()

			c.reduceTasks[reduceKey] = struct{}{}
		case <-c.reduceTaskCompletionTimer[reduceKey]:
			// Task complete
			c.mu.Lock()
			defer c.mu.Unlock()
		}

		timer.Stop()
	}()
}

func (c *Coordinator) GetTask(args *RequestTask, reply *ResponseTask) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.NReduce = c.nReduce

	// First handout map task if map is not empty
	if len(c.mapTasks) != 0 {
		reply.TaskType = MapType
		for key := range c.mapTasks {
			// TODO: pick a random key
			reply.InputFileOrPrefix = key
			break
		}

		delete(c.mapTasks, reply.InputFileOrPrefix)
		c.startMapTaskTimer(reply.InputFileOrPrefix)
		return nil
	}

	if len(c.reduceTasks) != 0 {
		reply.TaskType = ReduceType

		for bucketNumber := range c.reduceTasks {
			inputfiles, err := filepath.Glob(fmt.Sprintf("mr-out-*.txt-%d", bucketNumber))

			if err != nil {
				return err
			}

			if len(inputfiles) == c.mMap {
				reply.BucketNumber = bucketNumber
				delete(c.reduceTasks, reply.BucketNumber)
				c.startReduceTaskTimer(reply.BucketNumber)

				return nil
			}
		}

		reply.BucketNumber = -1
		return nil
	}

	return fmt.Errorf("Job completed")
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
	l, e := net.Listen("tcp", ":1234")
	// sockname := coordinatorSock()
	// os.Remove(sockname)
	// l, e := net.Listen("unix", sockname)
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

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, channel := range c.mapTaskCompletionTimer {
		select {
		case <-channel:
		default:
			return ret
		}
	}

	for _, channel := range c.reduceTaskCompletionTimer {
		select {
		case <-channel:
		default:
			return ret
		}
	}

	if len(c.mapTasks) == 0 && len(c.reduceTasks) == 0 {
		ret = true
	}

	return ret
}

func (c *Coordinator) CompleteMapTask(args *CompleteMapTaskRequest, reply *CompleteMapTaskResponse) error {
	close(c.mapTaskCompletionTimer[args.File])

	reply.Msg = "success"
	return nil
}

func (c *Coordinator) CompleteReduceTask(args *CompleteReduceTaskRequest, reply *CompleteReuduceTaskResponse) error {
	close(c.reduceTaskCompletionTimer[args.Bucket])

	reply.Msg = "success"
	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Keeping the nnumber of map workers will be the equal to the number of files

	// Your code here.
	// Create map tasks with files
	c.mapTaskCompletionTimer = make(map[string]chan struct{})
	c.reduceTaskCompletionTimer = make(map[int]chan struct{})

	c.createMapTasks(os.Args[1:])
	c.createReduceTasks(nReduce)

	c.nReduce = nReduce
	c.mMap = len(os.Args[1:])

	c.server()
	return &c
}
