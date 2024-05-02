package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
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

func Map(task ResponseTask, mapf func(string, string) []KeyValue) error {
	// this will be a single file
	// return fmt.Errorf("random error")
	content, err := os.ReadFile(task.InputFileOrPrefix)
	if err != nil {
		return err
	}

	kva := mapf(task.InputFileOrPrefix, string(content))

	// Will sort in reduce task
	sort.Sort(ByKey(kva))

	fileidx := make(map[int]*os.File)

	for i := 0; i < task.NReduce; i++ {
		filename := fmt.Sprintf("mr-out-%s-%d", filepath.Base(task.InputFileOrPrefix), i)

		outfile, err := os.CreateTemp("/tmp", filename)
		// outfile, err := os.Create(filename)
		if err != nil {
			return err
		}

		fileidx[i] = outfile
	}

	for _, kv := range kva {
		hashkey := ihash(kv.Key)

		outfile := fileidx[hashkey%task.NReduce]

		enc := json.NewEncoder(outfile)
		if err = enc.Encode(&kv); err != nil {
			return err
		}
	}

	// Rename all files
	for i, file := range fileidx {
		file.Close()

		if err := os.Rename(file.Name(), fmt.Sprintf("mr-out-%s-%d", filepath.Base(task.InputFileOrPrefix), i)); err != nil {
			return err
		}
	}

	return nil
}

func Reduce(task ResponseTask, reducef func(string, []string) string) error {
	outfilename := fmt.Sprintf("mr-out-%d", task.BucketNumber)

	// read all files
	// store their data in a []KeyValue arr
	// sort the arr
	// run reduce function on key and arr of values
	inputfiles, err := filepath.Glob(fmt.Sprintf("mr-out-*.txt-%d", task.BucketNumber))

	if err != nil {
		return err
	}

	if f, err := os.Stat(outfilename); f != nil && err == nil {
		return err
	}

	outfile, err := os.CreateTemp("/tmp", outfilename)
	// outfile, err := os.OpenFile(outfilename, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)

	if err != nil {
		return err
	}

	// defer outfile.Close()

	content := []KeyValue{}

	// Read from M files

	// if err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
	// 	if d.IsDir() {
	// 		return nil
	// 	}

	// 	if strings.HasPrefix(path, "mr-out-") && strings.HasSuffix(path, fmt.Sprintf(".txt-%d", task.BucketNumber)) {
	// 		file, err := os.OpenFile(path, os.O_RDONLY|os.O_EXCL, 0)

	// 		if err != nil {
	// 			return err
	// 		}

	// 		defer os.Remove(path)
	// 		// defer file.Close()

	// 		dec := json.NewDecoder(file)

	// 		for dec.More() {
	// 			var kv KeyValue

	// 			if err := dec.Decode(&kv); err != nil {
	// 				return err
	// 			}

	// 			// err != nil && err.Error() != "EOF" {
	// 			// 	return err
	// 			// } else if err != nil && err.Error() == "EOF" {
	// 			// 	break
	// 			// }

	// 			content = append(content, kv)
	// 		}
	// 	}
	// 	return nil
	// }); err != nil {
	// 	return err
	// }

	for _, intFile := range inputfiles {
		file, err := os.OpenFile(intFile, os.O_RDONLY|os.O_EXCL, 0)

		if err != nil {
			return err
		}

		defer os.Remove(intFile)
		// defer file.Close()

		dec := json.NewDecoder(file)

		for dec.More() {
			var kv KeyValue

			if err := dec.Decode(&kv); err != nil {
				return err
			}

			// err != nil && err.Error() != "EOF" {
			// 	return err
			// } else if err != nil && err.Error() == "EOF" {
			// 	break
			// }

			content = append(content, kv)
		}
	}

	sort.Sort(ByKey(content))

	// var i, j int

	// values := make([]string, 0)

	// for {
	// 	if j == len(content) {

	// 		fmt.Fprintf(outfile, "%v %v\n", content[i].Key, reducef(content[i].Key, values))
	// 		break
	// 	}
	// 	if content[i].Key == content[j].Key {
	// 		values = append(values, content[j].Value)
	// 		j++
	// 	} else {
	// 		fmt.Fprintf(outfile, "%v %v\n", content[i].Key, reducef(content[i].Key, values))
	// 		values = []string{}
	// 		i = j
	// 	}
	// }
	i := 0
	for i < len(content) {
		j := i + 1
		for j < len(content) && content[j].Key == content[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, content[k].Value)
		}
		output := reducef(content[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(outfile, "%v %v\n", content[i].Key, output)

		i = j
	}

	outfile.Close()

	if err := os.Rename(outfile.Name(), outfilename); err != nil {
		return err
	}

	return nil
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.
	//
	for {
		task, err := AskForTask()
		if err != nil {
			panic(err)
		}

		if task.BucketNumber == -1 {
			continue
		}

		switch task.TaskType {
		case MapType:
			err := Map(task, mapf)
			if err == nil {
				DoneMapTask(task.InputFileOrPrefix)
			} else {
				panic(err)
			}
		case ReduceType:
			err := Reduce(task, reducef)
			if err == nil {
				DoneReduceTask(task.BucketNumber)
			} else {
				panic(err)
			}
		}
	}
}

func DoneMapTask(file string) error {
	arg := CompleteMapTaskRequest{
		File: file,
	}

	reply := CompleteMapTaskResponse{}
	ok := call("Coordinator.CompleteMapTask", &arg, &reply)
	if ok {
		return nil
	} else {
		os.Exit(0)
		return fmt.Errorf("failed to get reply")
	}
}

func DoneReduceTask(bucket int) error {
	arg := CompleteReduceTaskRequest{
		Bucket: bucket,
	}

	reply := CompleteMapTaskResponse{}
	ok := call("Coordinator.CompleteReduceTask", &arg, &reply)
	if ok {
		return nil
	} else {
		os.Exit(0)
		return fmt.Errorf("failed to get reply")
	}
}

func AskForTask() (ResponseTask, error) {
	arg := RequestTask{}
	reply := ResponseTask{}
	ok := call("Coordinator.GetTask", &arg, &reply)
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
