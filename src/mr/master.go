package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	NMap            int
	NReduce         int
	workersLiving   int
	workersCount    int
	inputsPending   []*InterMediateFilePair       // not delivered [filepath]
	inputsDelivered map[int]*InterMediateFilePair // delivered {workerid:filepath}

	reduceptr       int
	reducePending   map[int][]*InterMediateFilePair //  map[Y]:[{X, filePath(mr-X-Y)}]
	reduceDelivered map[int]int                     // map[Y]:workerid

	outputFiles []string // [outputfiles(mr-output-Y)]

	workerStatuses map[int]*WorkerStatus
	mutex          sync.Mutex
}

// keep the records of every worker
type WorkerStage int

const (
	Mapping WorkerStage = iota
	Reducing
	Exit
)

const WatchTimeout = 5 * time.Second

type WorkerStatus struct {
	isIdle          bool
	workerStage     WorkerStage
	takeCareTaskID  int                     // for map/reduce stage, -1 for unset status
	takeCareFiles   []*InterMediateFilePair // for map/reduce stage, [] for unset status
	lastTimeVisited time.Time
}

func resetWS(ws WorkerStage) *WorkerStatus {
	return &WorkerStatus{
		isIdle:          true,
		workerStage:     ws,
		takeCareTaskID:  -1,
		takeCareFiles:   []*InterMediateFilePair{},
		lastTimeVisited: time.Now(),
	}
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

// register a worker before map starts
func (m *Master) RegisterWorker(req EmptyArgs, reply *MasterReplyArgs) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.workerStatuses[m.workersCount] = resetWS(Mapping)
	*reply = MasterReplyArgs{
		WorkerID:     m.workersCount,
		Command:      WAIT,
		ProcessFiles: []*InterMediateFilePair{},
		TaskId:       -1,
		NReduce:      m.NReduce,
	}
	// go m.observeWorker(m.workersCount)
	m.workersLiving += 1
	m.workersCount += 1

	return nil
}

func (m *Master) observeWorker(wid int) {
L:
	for {
		m.mutex.Lock()

		ws := m.workerStatuses[wid]
		if time.Since(ws.lastTimeVisited) > WatchTimeout {
			// reschedule the work
			if !ws.isIdle {
				switch ws.workerStage {
				case Mapping:
					m.inputsPending = append(m.inputsPending, ws.takeCareFiles...)
					delete(m.inputsDelivered, ws.takeCareTaskID)
				case Reducing:
					m.reducePending[ws.takeCareTaskID] = ws.takeCareFiles
					delete(m.reduceDelivered, ws.takeCareTaskID)
				case Exit:
					break L
				}
			}

			// remove the worker
			delete(m.workerStatuses, wid)
			m.workersLiving -= 1

			break L
		}

		m.mutex.Unlock()

		time.Sleep(WatchTimeout)
	}

}

func (m *Master) OnApplyForMapTask(req *WorkerRequestArgs, reply *MasterReplyArgs) error {
	// m.mutex.Lock()
	// defer m.mutex.Unlock()

	idx := req.WorkerID
	log.Println("Master OnApplyForMapTask:", len(m.inputsPending), " ", len(m.inputsDelivered))
	if len(m.inputsPending) == 0 {
		// All map tasks are Done
		if len(m.inputsDelivered) == 0 {
			m.workerStatuses[idx] = resetWS(Reducing)
			*reply = MasterReplyArgs{
				WorkerID:     idx,
				Command:      REDUCE,
				ProcessFiles: []*InterMediateFilePair{},
				TaskId:       -1,
			}
			return nil
		}

		// Wait for other Map nodes Done
		m.workerStatuses[idx] = resetWS(Mapping)
		*reply = MasterReplyArgs{
			WorkerID:     idx,
			Command:      WAIT,
			ProcessFiles: []*InterMediateFilePair{},
			TaskId:       -1,
		}
		return nil
	}
	imFile := m.inputsPending[0] // dequene

	m.workerStatuses[idx] = &WorkerStatus{
		isIdle:         false,
		workerStage:    Mapping,
		takeCareTaskID: imFile.X,
		takeCareFiles: []*InterMediateFilePair{
			imFile,
		},
	}

	m.inputsDelivered[imFile.X] = imFile

	log.Println("OnApplyForMapTask Before send reply:", reply)
	*reply = MasterReplyArgs{
		WorkerID:     idx,
		TaskId:       imFile.X,
		NReduce:      m.NReduce,
		Command:      MAP,
		ProcessFiles: []*InterMediateFilePair{imFile},
	}
	log.Println("OnApplyForMapTask After send reply:", reply)

	// dequeue at last is more safe
	m.inputsPending = m.inputsPending[1:]
	return nil
}

func (m *Master) OnMapWorkersDone(req *WorkerRequestArgs, reply *MasterReplyArgs) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	idx := req.WorkerID
	ws := m.workerStatuses[idx]
	delete(m.inputsDelivered, ws.takeCareTaskID)

	for _, output := range req.Outputs {
		m.reducePending[output.Y] = append(m.reducePending[output.Y], output)
	}

	m.workerStatuses[idx] = resetWS(Mapping)
	*reply = MasterReplyArgs{
		WorkerID:     idx,
		Command:      WAIT,
		ProcessFiles: []*InterMediateFilePair{},
		TaskId:       -1,
		NReduce:      m.NReduce,
	}
	return nil
}

func (m *Master) OnApplyForReduceTask(req *WorkerRequestArgs, reply *MasterReplyArgs) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	idx := req.WorkerID
	if len(m.reducePending) == 0 {
		// ALL Reduce tasks are Done
		if len(m.reduceDelivered) == 0 {
			m.workerStatuses[idx] = resetWS(Reducing)
			m.workerStatuses[idx].workerStage = Exit
			*reply = MasterReplyArgs{
				WorkerID:     idx,
				Command:      EXIT,
				ProcessFiles: []*InterMediateFilePair{},
				TaskId:       -1,
			}
			m.workersLiving -= 1
			return nil
		}

		// Wait for other Map nodes Done
		m.workerStatuses[idx] = resetWS(Reducing)
		*reply = MasterReplyArgs{
			WorkerID:     idx,
			Command:      WAIT,
			ProcessFiles: []*InterMediateFilePair{},
			TaskId:       -1,
		}
	}

	imFiles := m.reducePending[m.reduceptr] // dequene

	m.workerStatuses[idx] = &WorkerStatus{
		isIdle:         false,
		workerStage:    Reducing,
		takeCareTaskID: m.reduceptr,
		takeCareFiles:  imFiles,
	}

	m.reduceDelivered[m.reduceptr] = idx

	log.Println("OnApplyForReduceTask Before send reply:", reply)
	*reply = MasterReplyArgs{
		WorkerID:     idx,
		TaskId:       m.reduceptr,
		NReduce:      m.NReduce,
		Command:      REDUCE,
		ProcessFiles: imFiles,
	}
	log.Println("OnApplyForReduceTask After send reply:", reply)

	// dequeue at last is more safe
	delete(m.reducePending, m.reduceptr)
	m.reduceptr += 1
	return nil
}

func (m *Master) OnReduceWorkersDone(req *WorkerRequestArgs, reply *MasterReplyArgs) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	idx := req.WorkerID
	ws := m.workerStatuses[idx]
	delete(m.reduceDelivered, ws.takeCareTaskID)

	for _, output := range req.Outputs {
		m.outputFiles = append(m.outputFiles, output.FilePath)
	}

	m.workerStatuses[idx] = resetWS(Reducing)
	*reply = MasterReplyArgs{
		WorkerID:     idx,
		Command:      WAIT,
		ProcessFiles: []*InterMediateFilePair{},
		TaskId:       -1,
		NReduce:      m.NReduce,
	}
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
	m.mutex.Lock()
	defer m.mutex.Unlock()

	ret := false
	if len(m.workerStatuses) != 0 && m.workersLiving == 0 {
		ret = true
	}

	return ret
}

// number input files to meet map input requirments
func makeInputPairs(files []string) (imfp []*InterMediateFilePair) {
	for i, f := range files {
		imfp = append(imfp,
			&InterMediateFilePair{
				X:        i,
				Y:        -1,
				FilePath: f},
		)
	}
	return
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use, not map tasks
//

func MakeMaster(files []string, nReduce int) *Master {
	SetLogger("master")
	m := Master{
		NMap:            len(files),
		NReduce:         nReduce,
		inputsPending:   makeInputPairs(files),
		inputsDelivered: make(map[int]*InterMediateFilePair),
		reducePending:   make(map[int][]*InterMediateFilePair),
		reduceDelivered: make(map[int]int),
		outputFiles:     make([]string, 0),
		workerStatuses:  make(map[int]*WorkerStatus, 0),
		mutex:           sync.Mutex{}}

	// Your code here.
	// split files into nReduce pieces

	m.server()
	return &m
}
