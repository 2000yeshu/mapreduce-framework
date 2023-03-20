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
	"strconv"
)

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func Map(task ResponseTask, mapf func(string, string) []KeyValue) {
	// TODO: Get number of reduce tasks here
	noofpart := task.Task.ReduceTasks
	file, err := os.Open(task.Task.InputFile)
	if err != nil {
		panic(err)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		panic(err)
	}
	file.Close()

	// gives us intermediate kv
	kva := mapf(task.Task.InputFile, string(content))
	sort.Sort(ByKey(kva))

	fileidx := make(map[int]*os.File)

	for i := 0; i < noofpart; i++ {
		outfile, err := ioutil.TempFile("", fmt.Sprintf("temp-mr-%s-%s-", strconv.Itoa(task.Task.TaskNumber), strconv.Itoa(i)))
		if err != nil {
			panic(err)
		}

		fileidx[i] = outfile

		defer func(outfile *os.File, i int, task ResponseTask) {
			if err := outfile.Close(); err != nil {
				panic(err)
			}
			if err := os.Rename(outfile.Name(), fmt.Sprintf("mr-%s-%s", strconv.Itoa(task.Task.TaskNumber), strconv.Itoa(i))); err != nil {
				panic(err)
			}
		}(outfile, i, task)

	}

	for _, kv := range kva {
		reducehashkey := ihash(kv.Key)

		outfile := fileidx[reducehashkey%noofpart]
		enc := json.NewEncoder(outfile)
		if err = enc.Encode(&kv); err != nil {
			panic(err)
		}

	}
	TaskComplete(TaskTypes.Map, task.Task.TaskNumber)
	// for i := 0; i < noofpart; i++ {
	// 	err := os.Rename(fmt.Sprintf("mr-intm-val/temp-mr-%s-%s", strconv.Itoa(task.Task.TaskNumber), strconv.Itoa(i)), fmt.Sprintf("mr-intm-val/mr-%s-%s", strconv.Itoa(task.Task.TaskNumber), strconv.Itoa(i)))
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }

}

func Reduce(task ResponseTask, reducef func(string, []string) string) {

	// read all files
	// store their data in a []KeyValue arr
	// sort the arr
	// run reduce function on key and arr of values

	if _, err := os.Stat(task.Task.OutputFile); os.IsExist(err) {
		return
	}
	outfilename := task.Task.OutputFile
	outfile, err := os.OpenFile(outfilename, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}
	defer outfile.Close()

	content := []KeyValue{}

	for i := 1; i <= task.Task.MapTasks; i++ {
		immFileName := fmt.Sprintf("mr-%d-%d", i, task.Task.TaskNumber)
		file, err := os.OpenFile(immFileName, os.O_RDONLY|os.O_EXCL, 0)
		if err != nil {
			panic(err)
		}

		dec := json.NewDecoder(file)

		for {
			var kv KeyValue

			if err := dec.Decode(&kv); err != nil {
				break
			}

			content = append(content, kv)

		}
		file.Close()
	}

	sort.Sort(ByKey(content))

	prevKey := ""
	currkey := ""
	values := []string{}
	for _, kv := range content {
		currkey = kv.Key
		if prevKey == "" {
			prevKey = kv.Key
			values = []string{}
			values = append(values, kv.Value)
			continue
		}
		if prevKey == currkey {
			values = append(values, kv.Value)

		} else {

			// write prev key values to file

			fmt.Fprintf(outfile, "%v %v\n", prevKey, reducef(prevKey, values))

			prevKey = kv.Key
			values = []string{}
			values = append(values, kv.Value)
		}
	}
	// for writing the last value
	fmt.Fprintf(outfile, "%v %v\n", currkey, reducef(currkey, values))
	TaskComplete(TaskTypes.Reduce, task.Task.TaskNumber)

}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.
	//
	// fetch responsetask and execute it
	// again fetch and execute
	for {
		ok := JobCompleted()
		if !ok {
			return
		}
		task, err := AskForTask()
		if err != nil {
			continue
		}

		if task.Task.Type == TaskTypes.Map {
			TaskProgress(task.Task.Type, task.Task.TaskNumber)
			Map(task, mapf)
		} else if task.Task.Type == TaskTypes.Reduce {
			// check if all the files exist
			foundAllFiles := true
			// first check all files exist need for reduce task
			for i := 1; i <= task.Task.MapTasks; i++ {
				immFileName := fmt.Sprintf("mr-%d-%d", i, task.Task.TaskNumber)

				if _, err := os.Stat(immFileName); os.IsNotExist(err) {
					foundAllFiles = false
				}
			}
			if !foundAllFiles {
				TaskInit(task.Task.Type, task.Task.TaskNumber)
				continue
			}
			TaskProgress(task.Task.Type, task.Task.TaskNumber)
			Reduce(task, reducef)
		}
		//time.Sleep(2 * time.Second)

	}

}

func TaskComplete(taskType string, taskNumber int) {
	args := RequestTask{}
	args.Type = taskType
	args.TaskNumber = taskNumber

	reply := ResponseTask{}
	ok := call("Coordinator.TaskCompleted", &args, &reply)
	if !ok {
		os.Exit(0)
	}
}

func JobCompleted() bool {
	args := RequestTask{}
	reply := ResponseTask{}
	ok := call("Coordinator.JobCompleted", &args, &reply)
	return ok

}

func TaskProgress(taskType string, taskNumber int) {
	args := RequestTask{}
	args.Type = taskType
	args.TaskNumber = taskNumber

	reply := ResponseTask{}
	ok := call("Coordinator.TaskProgress", &args, &reply)
	if !ok {
		os.Exit(0)
	}
}

func TaskInit(taskType string, taskNumber int) {
	args := RequestTask{}
	args.Type = taskType
	args.TaskNumber = taskNumber

	reply := ResponseTask{}
	ok := call("Coordinator.TaskInit", &args, &reply)
	if !ok {
		os.Exit(0)
	}
}

func AskForTask() (ResponseTask, error) {
	args := RequestTask{}
	args.Type = "Task"

	reply := ResponseTask{}

	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		return reply, nil
	} else {
		os.Exit(0)
		return reply, fmt.Errorf("failed to get reply")
	}

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
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	// sockname := coordinatorSock()
	// c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
		os.Exit(0)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	return err == nil

}
