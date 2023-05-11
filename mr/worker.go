package mr

import (
	"bufio"
	"errors"
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

// Map functions return a slice of KeyValue.
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

type CompletedTask map[int][]string

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		task, err := CallGetTask()

		if err != nil {
			log.Fatal(err)
		}

		if task == nil {
			time.Sleep(time.Second)
			continue
		}

		if task.Type == "map" {
			input := readFile(task.Filename)
			intermediates := map[int][]KeyValue{}
			kva := mapf(task.Filename, input)
			for _, entry := range kva {
				bucket := spread(entry.Key, task.Reducers)
				intermediates[bucket] = append(intermediates[bucket], entry)
			}
			completed := CompletedTask{}
			for bucket, data := range intermediates {
				iname := "mr-inter-b" + strconv.Itoa(bucket) + "t" + task.Uid
				ifile, _ := os.Create(iname)
				for _, entry := range data {
					fmt.Fprintf(ifile, "%v %v\n", entry.Key, entry.Value)
				}
				ifile.Close()
				completed[bucket] = append(completed[bucket], iname)
			}
			CallSendCompleted(task.Uid, completed)
		}

		if task.Type == "reduce" {
			filenames := strings.Split(task.Filename, ";")
			intermediate := []KeyValue{}

			for _, filename := range filenames {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatal(err)
				}

				scanner := bufio.NewScanner(file)
				for scanner.Scan() {
					kvarr := strings.Split(scanner.Text(), " ")
					kv := KeyValue{Key: kvarr[0], Value: kvarr[1]}
					intermediate = append(intermediate, kv)
				}
				file.Close()

				if err := scanner.Err(); err != nil {
					log.Fatal(err)
				}
			}

			sort.Sort(ByKey(intermediate))

			oname := "mr-out-" + strconv.Itoa(task.Seq)
			ofile, _ := os.Create(oname)

			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-X.
			//
			for i, j := 0, 0; i < len(intermediate); i = j {
				j = i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
			}
			ofile.Close()
			CallSendCompleted(task.Uid, CompletedTask{})
		}

	}

}

func readFile(filename string) string {
	input, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(input)
	input.Close()
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	return string(content)
}

func CallGetTask() (*Task, error) {
	// declare an argument structure.
	args := GetTaskArgs{}
	// declare a reply structure.
	reply := GetTaskReply{}

	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		return nil, errors.New("cannot get task from server")
	}
	if reply.Task.Uid == "" {
		return nil, nil
	}
	return &reply.Task, nil
}

func CallSendCompleted(uid string, completed CompletedTask) error {
	// declare an argument structure.
	args := SendCompleteTaskArgs{Uid: uid, Files: completed}
	// declare a reply structure.
	reply := SendCompleteTaskReply{}

	ok := call("Coordinator.SendCompleted", &args, &reply)
	if !ok {
		return errors.New("cannot send completed tasks to server")
	}
	return nil
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		fmt.Println("Server shutdown or unreachable.")
		os.Exit(0)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func spread(key string, reducers int) int {
	return ihash(key) % reducers
}
