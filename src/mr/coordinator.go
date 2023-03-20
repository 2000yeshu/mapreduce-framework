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

// TODO: Handout reduce tasks along with map tasks to do the computing early on and
// give out responsibility to workers as early as possible

type Task struct {
	Type       string
	TaskNumber int
	InputFile  string
	OutputFile string

	// Should be locked
	Status string
	// Timer

	WorkerID string

	MapTasks    int
	ReduceTasks int
}

type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex

	// size: N*M
	Tasks []*Task

	MapTasks    int
	ReduceTasks int

	IsMapCompleted bool

	isJobCompleted bool

	// isMergeComplete bool

	JobComplete chan int
}

// Your code here -- RPC handlers for the worker to call.

// func (c *Coordinator) Merge() {
// 	// read all files
// 	// put everything in an arrat of KeyValue
// 	// sort, external if required
// 	// write to a file
// 	<-c.JobComplete
// 	files, err := ioutil.ReadDir("/temp-mr-out/")
// 	if err != nil {
// 		panic(err)
// 	}

// 	values := []KeyValue{}
// 	for _, file := range files {
// 		file, err := os.OpenFile(file.Name(), os.O_RDONLY|os.O_EXCL, 0)
// 		if err != nil {
// 			panic(err)
// 		}
// 		defer file.Close()
// 		dec := json.NewDecoder(file)
// 		for {
// 			var kv KeyValue
// 			if err := dec.Decode(&kv); err == io.EOF {
// 				break
// 			} else if err != nil {
// 				panic(err)
// 			}

// 			values = append(values, kv)
// 		}

// 	}

// 	sort.Sort(ByKey(values))

// 	prevKey := ""
// 	currkey := ""
// 	values := []string{}
// 	for {
// 		var kv KeyValue

// 		if err := dec.Decode(&kv); err == io.EOF {
// 			break
// 		} else if err != nil {
// 			panic(err)
// 		}
// 		currkey = kv.Key
// 		if prevKey == "" {
// 			prevKey = kv.Key
// 			values = []string{}
// 			values = append(values, kv.Value)
// 		}
// 		if prevKey == currkey {
// 			values = append(values, kv.Value)

// 		} else {

// 			// write prev key values to file
// 			if _, err := os.Stat(task.Task.OutputFile); os.IsExist(err) {
// 				fmt.Println("already exists, skipping", task.Task.OutputFile)
// 				break
// 			}
// 			outfilename := fmt.Sprintf("temp-%s", task.Task.OutputFile)
// 			outfile, err := os.OpenFile(outfilename, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
// 			if err != nil {
// 				panic(err)
// 			}

// 			// meaning prev value is not equal to curr val
// 			// write the reduce output to corresponding file
// 			enc := json.NewEncoder(outfile)
// 			err = enc.Encode(&KeyValue{
// 				Key:   prevKey,
// 				Value: reducef(prevKey, values),
// 			})
// 			if err != nil {
// 				panic(err)
// 			}
// 			outfile.Close()

// 			prevKey = kv.Key
// 			values = []string{}
// 			values = append(values, kv.Value)
// 		}
// 	}

// }

func (c *Coordinator) NotResponding(task *Task) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// switch task to init
	if task.Status == TaskStatuses.InProgress {
		fmt.Println("10 secs passed since task in progress. switching to init status", task)
		task.Status = TaskStatuses.Init
	}

}

func (c *Coordinator) GetTask(args *RequestTask, reply *ResponseTask) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.isJobCompleted {
		return fmt.Errorf("job completed")
	}

	doesNonCompleteJobExist := false

	for _, task := range c.Tasks {
		if task.Status != TaskStatuses.Completed {
			doesNonCompleteJobExist = true
		}
		if task.Status == TaskStatuses.Init && task.Status != TaskStatuses.HandedOut {
			reply.Task = *task
			task.Status = TaskStatuses.HandedOut

			timer := time.NewTimer(MaxUnavailable)
			go func(task *Task) {
				<-timer.C
				c.NotResponding(task)
			}(task)
			return nil
		}
	}

	if !doesNonCompleteJobExist {
		c.isJobCompleted = true
	}

	// please exit all workers
	// initiate merge

	return nil

}

func (c *Coordinator) TaskInit(args *RequestTask, reply *ResponseTask) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, task := range c.Tasks {
		if task.Type == args.Type && task.TaskNumber == args.TaskNumber && task.Status != TaskStatuses.Init {
			task.Status = TaskStatuses.Init
			reply.Task = *task

			break
		}
	}

	return nil
}

func (c *Coordinator) TaskProgress(args *RequestTask, reply *ResponseTask) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, task := range c.Tasks {
		if task.Type == args.Type && task.TaskNumber == args.TaskNumber && task.Status != TaskStatuses.InProgress {
			task.Status = TaskStatuses.InProgress
			reply.Task = *task
			break
		}
	}

	return nil
}

func (c *Coordinator) TaskCompleted(args *RequestTask, reply *ResponseTask) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, task := range c.Tasks {
		if task.Type == args.Type && task.TaskNumber == args.TaskNumber && task.Status != TaskStatuses.Completed {

			task.Status = TaskStatuses.Completed
			reply.Task = *task

			break
		}
	}

	return nil

}

func (c *Coordinator) JobCompleted(args *RequestTask, reply *ResponseTask) error {

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.isJobCompleted {
		return fmt.Errorf("Job completed")
	}
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
	if c.mu.Lock(); c.isJobCompleted {
		ret = true

	}
	c.mu.Unlock()

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	c := Coordinator{}
	c.IsMapCompleted = false
	c.isJobCompleted = false
	c.JobComplete = make(chan int)
	c.ReduceTasks = nReduce
	c.MapTasks = len(os.Args[1:])
	fmt.Println("number of map tasks:", len(os.Args[1:]), "number of reduce tasks:", os.Args[1:])

	// Your code here.
	// Create map tasks with files
	for mapidx, filename := range os.Args[1:] {
		temptask := &Task{}
		temptask.Status = TaskStatuses.Init

		temptask.Type = TaskTypes.Map
		temptask.TaskNumber = mapidx + 1
		temptask.InputFile = filename
		temptask.OutputFile = ""
		temptask.ReduceTasks = nReduce

		c.Tasks = append(c.Tasks, temptask)

	}
	for idx := 0; idx < c.ReduceTasks; idx++ {
		temptask := &Task{}
		temptask.Status = TaskStatuses.Init
		temptask.Type = TaskTypes.Reduce
		temptask.TaskNumber = idx
		temptask.MapTasks = len(os.Args[1:])
		//temptask.InputFile = fmt.Sprintf("mr-x-%s", strconv.Itoa(idx))
		temptask.OutputFile = fmt.Sprintf("mr-out-%d", idx)

		c.Tasks = append(c.Tasks, temptask)

	}
	// wait for them to complete

	c.server()
	fmt.Println("server started")
	return &c
}
