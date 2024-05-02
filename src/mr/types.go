package mr

import "time"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

var MaxUnavailable time.Duration = 10 * time.Second
