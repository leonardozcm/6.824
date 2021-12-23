package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Master struct {
	// Your definitions here.
	NReduce         int
	inputsPending   []string // not delivered
	inputsDelivered []string // delivered
	workerStatuses  []*WorkerStatus
	mutex           sync.RWMutex
}

// keep the records of every worker
type WorkerStatus struct {
	isIdle     bool
	mapFile    string // for map stage
	reducePart int    // for reduce -1 for unset status
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// register a worker before map starts
func (m *Master) RegisterWorker(args *ExampleArgs, idx *int) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.workerStatuses = append(m.workerStatuses, &WorkerStatus{
		isIdle:     true,
		mapFile:    "",
		reducePart: -1,
	})
	*idx = len(m.workerStatuses) - 1

	return nil

}

func (m *Master) ApplyForTask(idx int, path *string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if len(m.inputsPending) == 0 {
		path = nil
		return errors.New("there is no task pending")
	}
	*path = m.inputsPending[0]
	m.workerStatuses[idx].isIdle = false
	m.workerStatuses[idx].mapFile = *path

	m.inputsDelivered = append(m.inputsDelivered, *path)

	// dequeue at last is more safe
	m.inputsPending = m.inputsPending[1:]
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use, not map tasks
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{NReduce: nReduce,
		inputsPending:   files,
		inputsDelivered: []string{},
		workerStatuses:  []*WorkerStatus{},
		mutex:           sync.RWMutex{}}

	// Your code here.
	// split files into nReduce pieces

	m.server()
	return &m
}
