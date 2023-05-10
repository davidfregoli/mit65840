package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Task struct {
	Filename string
	Reducers int
	Seq      int
	Started  int64
	State    int
	Type     string
	Uid      string
}

type Coordinator struct {
	Phase    string
	Queue    []Task
	QueueMu  sync.Mutex
	Reducers int
	Shuffles map[int][]string
}

var increment int = 1
var muIncrement sync.Mutex

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.QueueMu.Lock()
	defer c.QueueMu.Unlock()
	for i := range c.Queue {
		task := &c.Queue[i]
		if task.State == 0 && task.Type == c.Phase {
			task.State = 1
			task.Started = time.Now().Unix()
			reply.Task = *task
			break
		}
	}
	return nil
}

func (c *Coordinator) SendCompleted(args *SendCompleteTaskArgs, reply *SendCompleteTaskReply) error {
	c.QueueMu.Lock()
	defer c.QueueMu.Unlock()
	for i := range c.Queue {
		task := &c.Queue[i]
		if task.Uid != args.Uid {
			continue
		}
		if task.State != 1 {
			break
		}
		task.State = 2
		if task.Type != "map" {
			break
		}
		for bucket, files := range args.Files {
			c.Shuffles[bucket] = append(c.Shuffles[bucket], files...)
		}
	}
	return nil
}

func (c *Coordinator) Loop() {
	for {
		time.Sleep(time.Second / 5)
		c.Tick()
	}
}

func (c *Coordinator) Tick() {
	c.QueueMu.Lock()
	defer c.QueueMu.Unlock()
	now := time.Now().Unix()
	allDone := true
	for i := range c.Queue {
		task := &c.Queue[i]
		if task.State != 2 {
			allDone = false
		}
		if task.State != 1 {
			continue
		}
		diff := now - task.Started
		if diff > 10 {
			task.Started = 0
			task.State = 0
			task.Uid = uid()
		}
	}
	if allDone && c.Phase == "map" {
		c.Phase = "reduce"
		for bucket, files := range c.Shuffles {
			task := Task{Filename: strings.Join(files, ";"), Seq: bucket, Type: "reduce", Uid: uid()}
			c.Queue = append(c.Queue, task)
		}
	} else if allDone && c.Phase == "reduce" {
		c.Phase = "done"
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.Phase == "done"
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{Reducers: nReduce, Phase: "map"}
	for _, filename := range files {
		task := Task{Filename: filename, Reducers: nReduce, Type: "map", Uid: uid()}
		c.Queue = append(c.Queue, task)
	}
	c.Shuffles = map[int][]string{}
	c.server()
	go c.Loop()
	return &c
}

func uid() string {
	muIncrement.Lock()
	defer muIncrement.Unlock()
	out := increment
	increment += 1
	return strconv.Itoa(out)
}
