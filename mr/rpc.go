package mr

import (
	"os"
	"strconv"
)

type SendCompleteTaskArgs struct {
	Uid   string
	Files CompletedTask
}
type SendCompleteTaskReply struct{}

type GetFileArgs struct{}

type GetFileReply struct {
	File        string
	ReduceCount int
}
type GetTaskArgs struct{}
type GetTaskReply struct {
	Task
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
