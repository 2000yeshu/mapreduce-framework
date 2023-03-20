package mr

import "time"

type TaskStatus struct {
	Init, HandedOut, InProgress, Completed string
}

var TaskStatuses = TaskStatus{
	Init:       "init",
	HandedOut:  "handedout",
	InProgress: "inprogress",
	Completed:  "completed",
}

type TaskType struct {
	Map, Reduce string
}

var TaskTypes = TaskType{
	Map:    "Map",
	Reduce: "Reduce",
}

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

var MaxUnavailable time.Duration = 10 * time.Second
