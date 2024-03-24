package mr

import "encoding/json"
import "fmt"
import "io/ioutil"
import "log"
import "net/rpc"
import "os"
import "time"
import "hash/fnv"

// For sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// KeyValue represents a key-value pair.
type KeyValue struct {
	Key   string
	Value string
}

// ihash computes the hash of a string key.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker is responsible for executing Map and Reduce tasks.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		var task Task 
		args := ReqTaskArgs{}
		reply := ReqTaskReply{}

		call("Coordinator.RequestTask", &args, &reply)
		
		task = reply.Task

		if task.Type == MAP {
			handleMap(&task, mapf)
			TaskDone(task.ID)
		} else if task.Type == REDUCE {
			handleReduce(&task, reducef)
			TaskDone(task.ID)
		} else if task.Type == NO_TASK {
			// No action needed for NO_TASK
		} else if task.Type == QUIT {
			TaskDone(task.ID)
			time.Sleep(time.Second)
			os.Exit(0)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TaskDone informs the coordinator that a task is done.
func TaskDone(id int) {
	args := TaskFinishedArgs{id}
	reply := TaskFinishedReply{}
	call("Coordinator.TaskDone", &args, &reply)
}

// handleMap processes a Map task.
func handleMap(task *Task, mapf func(string, string) []KeyValue) {
	filename := task.FileName

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Cannot open %v", filename)
	}
	defer file.Close()

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("Cannot read %v", filename)
	}

	kva := mapf(filename, string(content))
	buckets := make(map[int]map[string][]string)
	nReduce := task.NReduce

	for _, kv := range kva {
		index := ihash(kv.Key) % nReduce
		if buckets[index] == nil {
			buckets[index] = make(map[string][]string)
		}
		buckets[index][kv.Key] = append(buckets[index][kv.Key], kv.Value)
	}

	for i := 0; i < nReduce; i++ {
		rFile, err := os.Create(getTempFileName(task.ID, i))
		if err != nil {
			log.Println("Failed to create file!")
		}
		enc := json.NewEncoder(rFile)
		kvs := buckets[i]
		enc.Encode(kvs)
		rFile.Close()
	}
}

func handleReduce(task *Task, reducef func(string, []string) string) {
	nMap := task.NMap
	intermediate := make(map[string][]string)

	for i := 0; i < nMap; i++ {
		rFile, err := os.Open(getTempFileName(i, task.ID))
		if err != nil {
			log.Println("Failed to open file:", err)
			continue
		}
		defer rFile.Close()

		dec := json.NewDecoder(rFile)
		var kvs map[string][]string
		if err := dec.Decode(&kvs); err != nil {
			log.Println("Failed to decode kvs! Error:", err)
			continue
		}

		for key, values := range kvs {
			intermediate[key] = append(intermediate[key], values...)
		}
	}

	ofile, err := os.Create(getOutputFileName(task.ID))
	if err != nil {
		log.Println("Failed to create output file:", err)
		return
	}
	defer ofile.Close()

	for key, values := range intermediate {
		output := reducef(key, values)
		fmt.Fprintf(ofile, "%v %v\n", key, output)
	}

	removeTemporaryFiles(nMap, task.ID)
}

func removeTemporaryFiles(nMap int, taskID int) {
    for i := 0; i < nMap; i++ {
        err := os.Remove(getTempFileName(i, taskID))
        if err != nil {
            log.Printf("Failed to remove temporary file %d for task %s: %v\n", i, taskID, err)
        }
    }
}

func getTempFileName(mapNumber int, reduceNumber int) string {
	return fmt.Sprintf("mr-%d-%d", mapNumber, reduceNumber)
}

func getOutputFileName(reduceNumber int) string {
	return fmt.Sprintf("mr-out-%d", reduceNumber)
}

// CallExample is an example function to make an RPC call to the coordinator.
func CallExample() {
	args := ExampleArgs{}
	args.X = 99
	reply := ExampleReply{}

	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Println("Call failed!")
	}
}

// call sends an RPC request to the coordinator.
func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("Dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
