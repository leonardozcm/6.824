package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type WorkerInfo struct {
	wid     int
	status  CommandType
	nReduce int
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// 1. register this worker process to master
	worker := Register()
	worker.mapf = mapf
	worker.reducef = reducef

	// 2. Map stage
	reply := &MasterReplyArgs{}
M:
	for {
		switch worker.status {
		case WAIT:
			time.Sleep(time.Second)
			ApplyForMapTask(worker, reply)
		case MAP:
			MapOps(worker, reply)
			*reply = MasterReplyArgs{} // it is necessary to remalloc reply space
		case REDUCE:
			break M
		}
	}

	// 3. Reduce stage
	worker.status = WAIT
	*reply = MasterReplyArgs{}

R:
	for {
		switch worker.status {
		case WAIT:
			time.Sleep(time.Second)
			ApplyForReduceTask(worker, reply)
		case REDUCE:
			ReduceOps(worker, reply)
			*reply = MasterReplyArgs{}
		case EXIT:
			break R
		}
	}

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func Register() *WorkerInfo {
	args := EmptyArgs{}
	msg := "register this worker."
	args.Msg = msg
	reply := MasterReplyArgs{}
	call("Master.RegisterWorker", args, &reply)

	return &WorkerInfo{
		wid:     reply.WorkerID,
		status:  reply.Command,
		nReduce: reply.NReduce,
	}
}

func ApplyForMapTask(worker *WorkerInfo, reply *MasterReplyArgs) {
	req := WorkerRequestArgs{
		WorkerID: worker.wid,
	}

	log.Println("Worker Before ApplyForMapTask:", reply)
	call("Master.OnApplyForMapTask", &req, reply)
	log.Println("Worker After ApplyForMapTask:", reply)
	worker.status = reply.Command
}

func ApplyForReduceTask(worker *WorkerInfo, reply *MasterReplyArgs) {
	req := WorkerRequestArgs{
		WorkerID: worker.wid,
	}

	log.Println("Worker Before ApplyForReduceTask:", reply)
	call("Master.OnApplyForReduceTask", &req, reply)
	log.Println("Worker After ApplyForReduceTask:", reply)
	worker.status = reply.Command
}

func MapOps(worker *WorkerInfo, reply *MasterReplyArgs) {
	intermediate := []KeyValue{}
	log.Println("rcv reply:", reply)
	log.Println("rcv imFile:", reply.ProcessFiles[0])
	for _, pf := range reply.ProcessFiles {
		filename := pf.FilePath
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := worker.mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	// write to mr-X-Y
	X := reply.TaskId
	imFileName := "mr-tmp/mr-%d-%d"
	imFileKVMap := make(map[int][]KeyValue)
	// 1. Process kvs 2 map
	for _, kv := range intermediate {
		imFileKVMap[ihash(kv.Key)%worker.nReduce] = append(imFileKVMap[ihash(kv.Key)%worker.nReduce], kv)
	}

	// 2. Write 2 files
	outputsFiles := make([]*InterMediateFilePair, 0, worker.nReduce)
	for Y, v := range imFileKVMap {
		oname := fmt.Sprintf(imFileName, X, Y)
		ofile, _ := os.Create(oname)

		i := 0
		for i < len(v) {
			fmt.Fprintf(ofile, "%v %v\n", v[i].Key, v[i].Value)
			i += 1
		}
		ofile.Close()
		outputsFiles = append(outputsFiles,
			&InterMediateFilePair{
				X:        X,
				Y:        Y,
				FilePath: oname,
			},
		)
	}

	// info master that works done
	req := WorkerRequestArgs{
		WorkerID: worker.wid,
		Outputs:  outputsFiles,
	}
	call("Master.OnMapWorkersDone", &req, reply)
	worker.status = reply.Command
	log.Println("Worker MapOps:", worker.status)

}

func ReduceOps(worker *WorkerInfo, reply *MasterReplyArgs) {
	intermediate := []KeyValue{}
	log.Println("Reduce rcv reply:", reply)
	log.Println("Reduce rcv imFile:", reply.ProcessFiles[0])
	for _, pf := range reply.ProcessFiles {
		filename := pf.FilePath
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		for _, kv := range strings.Split(string(content), "\n") {
			if kv == "" {
				continue
			}
			kvtmp := strings.Split(kv, " ")
			// todo: check legalness
			intermediate = append(intermediate, KeyValue{kvtmp[0], kvtmp[1]})
		}
	}
	sort.Sort(ByKey(intermediate))

	// write to mr-output-Y
	Y := reply.TaskId
	outputFileName := "mr-tmp/mr-output-%d"
	oname := fmt.Sprintf(outputFileName, Y)
	ofile, _ := os.Create(oname)
	i := 0
	start := 0
	outputsFiles := make([]*InterMediateFilePair, 0, worker.nReduce)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	for i < len(intermediate) {
		j := i + 1
		start = j
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := worker.reducef(intermediate[i].Key, values)

		if output_i, _ := strconv.Atoi(output); j-start+1 != output_i {
			fmt.Printf("index j increase %d, output is %s\n", j-start, output)
		}

		// this is the correct format for each line of Reduce output.

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()
	outputsFiles = append(outputsFiles,
		&InterMediateFilePair{
			X:        -1,
			Y:        Y,
			FilePath: oname,
		},
	)

	// info master that works done
	req := WorkerRequestArgs{
		WorkerID: worker.wid,
		Outputs:  outputsFiles,
	}
	call("Master.OnReduceWorkersDone", &req, reply)
	worker.status = reply.Command
	log.Println("Worker ReduceOps:", worker.status)

}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	log.Println(err)
	return false
}
