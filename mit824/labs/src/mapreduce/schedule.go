package mapreduce

import (
		"fmt"
		"sync"
)

const FINISHED = 0
const INPROGRESS = 1
const UNFINISHED = 2

func doTask(wChan chan string, wgroup *sync.WaitGroup,
	jobId int, jobStates []int, args DoTaskArgs) {

	workerAddr := <- wChan
	workCompleted := call(workerAddr, "Worker.DoTask", args, nil)
	if (workCompleted) {
		wgroup.Done()
	} else {
		go doTask(wChan, wgroup, jobId, jobStates, args)
	}
	wChan <- workerAddr
	// why must put wgroup.Done() before wChan <- workerAddr
	// because put worker into the channel may be blocked
}

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)
	fmt.Printf("Channel size: %d\n", cap(registerChan))
	jobStates := make([]int, ntasks)
	for i, _ := range jobStates {
		jobStates[i] = UNFINISHED
	}

	var wg sync.WaitGroup
	for jobId := 0; jobId < ntasks; jobId++ {
		var args DoTaskArgs
		if (phase == mapPhase) {
			args = DoTaskArgs{jobName, mapFiles[jobId], mapPhase, jobId, n_other}
		} else {
			args = DoTaskArgs{jobName, "", reducePhase, jobId, n_other}
		}
		wg.Add(1)
		go doTask(registerChan, &wg, jobId, jobStates, args)
	}
	wg.Wait()

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	fmt.Printf("Schedule: %v done\n", phase)
}
