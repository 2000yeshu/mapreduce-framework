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

type CompleteMapTaskRequest struct {
	File string
}
type CompleteMapTaskResponse struct {
	Msg string
}

type CompleteReduceTaskRequest struct {
	Bucket int
}
type CompleteReuduceTaskResponse struct {
	Msg string
}

type RequestTask struct{}

type TaskType int

const (
	MapType TaskType = iota
	ReduceType
)

type ResponseTask struct {
	TaskType          TaskType
	InputFileOrPrefix string
	BucketNumber      int
	NReduce           int
}

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

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
