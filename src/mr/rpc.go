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

// Define msgs when a worker apply worker from master

type CommandType int

type InterMediateFilePair struct {
	X        int
	Y        int
	FilePath string
}

const (
	MAP CommandType = iota
	REDUCE
	WAIT
	EXIT
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type EmptyArgs struct {
	Msg string
}

type MasterReplyArgs struct {
	WorkerID     int
	TaskId       int
	NReduce      int
	Command      CommandType
	ProcessFiles []*InterMediateFilePair
}

type WorkerRequestArgs struct {
	WorkerID int
	Outputs  []*InterMediateFilePair
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
