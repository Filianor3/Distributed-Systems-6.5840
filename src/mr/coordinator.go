package mr

import "log"
import "math"
import "sync"
import "time"
import "net"
import "os"
import "net/rpc"
import "net/http"

const Debug = false


type Coordinator struct {
	// Your definitions here.
	state int // Records the current stage
	nReduce int // Number of reduce tasks
	nMap int // Number of map tasks
	taskQueue chan *Task // Task queue
	taskMap map[int]*Task // Task list
	mu sync.Mutex // Lock
}

type TaskType int

const (
	MAP = 1
	REDUCE = 2
	NO_TASK = 0
	QUIT = -1
)

type Task struct {
	ID int
	Type TaskType
	FileName string
	NReduce int
	NMap int
	Deadline int64
}

// Your code here -- RPC handlers for the worker to call.

//
// An example RPC handler.
//
// The RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// Start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	c.mu.Lock()
	ret = c.state == QUIT
	c.mu.Unlock()
	return ret
}

//
// Create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.nReduce = nReduce
	c.nMap = len(files)
	queueSize := int(math.Max(float64(len(files)), float64(nReduce))) 
	c.taskQueue = make(chan *Task, queueSize)
	c.taskMap = make(map[int]*Task)
	c.mu = sync.Mutex{}
	c.state = MAP

	for i, filename := range files {
		task := Task{
			ID:       i,
			Type:     MAP,
			FileName: filename,
			NReduce:  c.nReduce,
			NMap:     c.nMap,
			Deadline: -1,
		}
		c.taskQueue <- &task
		c.taskMap[i] = &task
	}

	go c.RunTasks()

	c.server()
	return &c
}

func (c *Coordinator) RunTasks() {
	for {
		c.mu.Lock()
		if len(c.taskMap) == 0 {
			c.changeState()
		} else {
			c.checkTaskTimeout()
		}
		c.mu.Unlock()

		time.Sleep(100 * time.Millisecond)
	}
}

func (c *Coordinator) checkTaskTimeout() {
	var timedOutTasks []*Task // this will store the timeouted tasks
	
	for _, task := range c.taskMap {
		if task.Deadline != -1 && time.Now().Unix() > task.Deadline {
			task.Deadline = -1
			timedOutTasks = append(timedOutTasks, task)
		}
	}

	// add the tasks that were timeouted back to the channel 
	for _, task := range timedOutTasks {
		c.taskQueue <- task
	}
}

func (c *Coordinator) changeState() {
	if c.state == MAP {
		c.state = REDUCE
		c.generateReduceTasks()
	} else if c.state == REDUCE {
		c.state = QUIT
		c.generateQuitTasks()
	} else if c.state == QUIT {
		os.Exit(0)
	}
}

func (c *Coordinator) generateReduceTasks() {
	c.taskMap = make(map[int]*Task)
	for i := 0; i < c.nReduce; i++ {
		createTask(c, i, REDUCE)
	}
}

func (c *Coordinator) generateQuitTasks() {
	for i := 0; i < c.nReduce; i++ {
		createTask(c, i, QUIT)
	}
}

func createTask(c *Coordinator, i int, taskType TaskType) {
    task := Task{
        ID:       i,
        Type:     taskType,
        NReduce:  c.nReduce,
        NMap:     c.nMap,
        Deadline: -1,
    }
    c.taskQueue <- &task
    c.taskMap[i] = &task
}

func (c *Coordinator) RequestTask(args *ReqTaskArgs, reply *ReqTaskReply) error {
	if len(c.taskMap) != 0 {
		task := <-c.taskQueue
		task.Deadline = time.Now().Add(10 * time.Second).Unix()
		reply.Task = *task
	} else {
		reply.Task = Task{Type: NO_TASK}
	}
	return nil
}

func (c *Coordinator) TaskDone(args *TaskFinishedArgs, reply *TaskFinishedReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.taskMap, args.ID)
	return nil
}
