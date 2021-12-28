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

	reducePending   map[int][]*InterMediateFilePair //  map[Y]:[{X, Y, filePath(mr-X-Y)}]
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
	isAlive         bool
	workerStage     WorkerStage
	takeCareTaskID  int                     // for map/reduce stage, -1 for unset status
	takeCareFiles   []*InterMediateFilePair // for map/reduce stage, [] for unset status
	lastTimeVisited time.Time
}

func resetWS(ws *WorkerStatus, stage WorkerStage) {
	ws.isIdle = true
	ws.workerStage = stage
	ws.takeCareTaskID = -1
	ws.takeCareFiles = []*InterMediateFilePair{}
	ws.lastTimeVisited = time.Now()
	ws.isAlive = true

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

	m.workerStatuses[m.workersCount] = &WorkerStatus{
		isIdle:          true,
		workerStage:     Mapping,
		takeCareTaskID:  -1,
		takeCareFiles:   []*InterMediateFilePair{},
		lastTimeVisited: time.Now(),
		isAlive:         true,
	}
	*reply = MasterReplyArgs{
		WorkerID:     m.workersCount,
		Command:      WAIT,
		ProcessFiles: []*InterMediateFilePair{},
		TaskId:       -1,
		NReduce:      m.NReduce,
	}
	log.Println("Registed wid: ", m.workersCount)
	go m.observeWorker(m.workersCount)
	m.workersLiving += 1
	m.workersCount += 1

	return nil
}

func (m *Master) observeWorker(wid int) {
L:
	for {
		time.Sleep(WatchTimeout)
		m.mutex.Lock()

		ws := m.workerStatuses[wid]
		log.Println("wid", wid, " time duraing", time.Since(ws.lastTimeVisited))
		if time.Since(ws.lastTimeVisited) > WatchTimeout {
			// reschedule the work
			if !ws.isIdle {
				log.Printf("Crash happend, wid %d, Info %+v\n", wid, ws)
				switch ws.workerStage {
				case Mapping:
					m.inputsPending = append(m.inputsPending, ws.takeCareFiles...)
					delete(m.inputsDelivered, ws.takeCareTaskID)
				case Reducing:
					m.reducePending[ws.takeCareTaskID] = ws.takeCareFiles
					delete(m.reduceDelivered, ws.takeCareTaskID)
				}
			} else {
				log.Printf("Exit happend, wid %d, Info %+v\n", wid, ws)
			}

			// remove the worker
			m.workerStatuses[wid].isAlive = false
			m.workersLiving -= 1
			m.mutex.Unlock()
			break L
		}

		m.mutex.Unlock()

	}

}

func (m *Master) OnApplyForMapTask(req *WorkerRequestArgs, reply *MasterReplyArgs) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	idx := req.WorkerID

	log.Println("wid", req.WorkerID, " OnApplyForMapTask:", len(m.inputsPending), " ", len(m.inputsDelivered),
		" ", len(m.reducePending), " ", len(m.reduceDelivered))

	if len(m.inputsPending) == 0 {
		// All map tasks are Done
		if len(m.inputsDelivered) == 0 {
			resetWS(m.workerStatuses[idx], Reducing)
			*reply = MasterReplyArgs{
				WorkerID:     idx,
				Command:      REDUCE,
				ProcessFiles: []*InterMediateFilePair{},
				TaskId:       -1,
			}
			return nil
		}

		// Wait for other Map nodes Done
		resetWS(m.workerStatuses[idx], Mapping)
		*reply = MasterReplyArgs{
			WorkerID:     idx,
			Command:      WAIT,
			ProcessFiles: []*InterMediateFilePair{},
			TaskId:       -1,
		}
		return nil
	}
	imFile := m.inputsPending[0] // dequene

	// store task infos in workerstatus
	ws := m.workerStatuses[idx]
	ws.isIdle = false
	ws.workerStage = Mapping
	ws.takeCareTaskID = imFile.X
	ws.takeCareFiles = []*InterMediateFilePair{
		imFile,
	}
	ws.lastTimeVisited = time.Now()

	m.inputsDelivered[imFile.X] = imFile

	*reply = MasterReplyArgs{
		WorkerID:     idx,
		TaskId:       imFile.X,
		NReduce:      m.NReduce,
		Command:      MAP,
		ProcessFiles: []*InterMediateFilePair{imFile},
	}
	log.Printf("wid %d OnApplyForMapTask After send reply: %+v\n", req.WorkerID, reply)

	// dequeue at last is more safe
	m.inputsPending = m.inputsPending[1:]
	return nil
}

func (m *Master) OnMapWorkersDone(req *WorkerRequestArgs, reply *MasterReplyArgs) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	log.Println("wid", req.WorkerID, " OnMapWorkersDone:", len(m.inputsPending), " ", len(m.inputsDelivered),
		" ", len(m.reducePending), " ", len(m.reduceDelivered))
	idx := req.WorkerID
	ws := m.workerStatuses[idx]
	if !ws.isAlive {
		// Abort this req, and let it waiting
		m.workersLiving += 1
		go m.observeWorker(idx)
	} else {
		delete(m.inputsDelivered, ws.takeCareTaskID)

		for _, output := range req.Outputs {
			m.reducePending[output.Y] = append(m.reducePending[output.Y], output)
		}
	}

	resetWS(ws, Mapping)
	*reply = MasterReplyArgs{
		WorkerID:     idx,
		Command:      WAIT,
		ProcessFiles: []*InterMediateFilePair{},
		TaskId:       -1,
		NReduce:      m.NReduce,
	}
	log.Printf("wid %d OnMapWorkersDone After send reply: %+v\n", req.WorkerID, reply)
	return nil
}

func (m *Master) OnApplyForReduceTask(req *WorkerRequestArgs, reply *MasterReplyArgs) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	log.Println("wid", req.WorkerID, " OnApplyForReduceTask:", len(m.inputsPending), " ", len(m.inputsDelivered),
		" ", len(m.reducePending), " ", len(m.reduceDelivered))

	idx := req.WorkerID
	ws := m.workerStatuses[idx]
	if len(m.reducePending) == 0 {
		// ALL Reduce tasks are Done
		if len(m.reduceDelivered) == 0 {
			resetWS(ws, Reducing)
			ws.workerStage = Exit
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
		resetWS(ws, Reducing)
		*reply = MasterReplyArgs{
			WorkerID:     idx,
			Command:      WAIT,
			ProcessFiles: []*InterMediateFilePair{},
			TaskId:       -1,
		}
	}

	for Y, v := range m.reducePending {
		ws.isIdle = false
		ws.workerStage = Reducing
		ws.takeCareTaskID = Y
		ws.takeCareFiles = v
		ws.lastTimeVisited = time.Now()

		m.reduceDelivered[Y] = idx

		*reply = MasterReplyArgs{
			WorkerID:     idx,
			TaskId:       Y,
			NReduce:      m.NReduce,
			Command:      REDUCE,
			ProcessFiles: v,
		}
		log.Printf("wid %d OnApplyForReduceTask After send reply: %+v\n", req.WorkerID, reply)

		// dequeue at last is more safe
		delete(m.reducePending, Y)
		break
	}

	return nil
}

func (m *Master) OnReduceWorkersDone(req *WorkerRequestArgs, reply *MasterReplyArgs) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	log.Println("wid", req.WorkerID, " OnReduceWorkersDone:", len(m.inputsPending), " ", len(m.inputsDelivered),
		" ", len(m.reducePending), " ", len(m.reduceDelivered))
	idx := req.WorkerID
	ws := m.workerStatuses[idx]
	if !ws.isAlive {
		m.workersLiving += 1
		go m.observeWorker(idx)
	} else {
		delete(m.reduceDelivered, ws.takeCareTaskID)

		for _, output := range req.Outputs {
			m.outputFiles = append(m.outputFiles, output.FilePath)
		}
	}

	resetWS(ws, Reducing)
	*reply = MasterReplyArgs{
		WorkerID:     idx,
		Command:      WAIT,
		ProcessFiles: []*InterMediateFilePair{},
		TaskId:       -1,
		NReduce:      m.NReduce,
	}
	log.Printf("wid %d OnReduceWorkersDone After send reply: %+v\n", req.WorkerID, reply)
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
	if len(m.workerStatuses) != 0 && m.workersLiving == 0 &&
		len(m.inputsDelivered)+len(m.inputsPending)+len(m.reducePending)+len(m.reduceDelivered) == 0 {
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
		workersLiving:   0,
		workersCount:    0,
		inputsPending:   makeInputPairs(files),
		inputsDelivered: make(map[int]*InterMediateFilePair),

		reducePending:   make(map[int][]*InterMediateFilePair),
		reduceDelivered: make(map[int]int),

		outputFiles: make([]string, 0),

		workerStatuses: make(map[int]*WorkerStatus, 0),
		mutex:          sync.Mutex{},
	}

	// Your code here.
	// split files into nReduce pieces

	m.server()
	return &m
}
