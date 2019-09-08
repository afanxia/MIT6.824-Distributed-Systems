package mapreduce

import (
	"fmt"
	"sync"
)

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

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	var (
		q        []int
		mu       *sync.Mutex
		complete bool
	)
	// init task queue
	for i := 0; i < ntasks; i++ {
		q = append(q, 0)
	}
	mu = &sync.Mutex{}

	for {
		// var wg sync.WaitGroup
		wg := &sync.WaitGroup{}

		// if complete, all tasks are done successfully.
		complete = true
		mu.Lock()
		for i := 0; i < ntasks; i++ {
			if q[i] == 0 {
				complete = false
				break
			}
		}
		mu.Unlock()
		if complete {
			break
		}

		// schedule
		for i := 0; i < ntasks; i++ {
			mu.Lock()
			if q[i] == 1 {
				mu.Unlock()
				continue
			}
			mu.Unlock()

			select {
			case worker := <-registerChan:
				wg.Add(1)
				// Launch a goroutine to do map/reduce.
				go func(wg *sync.WaitGroup, i int) {
					args := &DoTaskArgs{
						JobName:       jobName,
						File:          mapFiles[i],
						Phase:         phase,
						TaskNumber:    i,
						NumOtherPhase: n_other,
					}

					// Do real tasks
					ok := call(worker, "Worker.DoTask", args, nil)
					if ok == false {
						fmt.Printf("Worker: RPC %s DoTask %d error, will re-assign it later\n", worker, i)
					} else {
						mu.Lock()
						q[i] = 1
						mu.Unlock()
					}
					// After current worker is done, no matter failed or not, will do following 2 steps.
					// 1. Decrement the counter when the goroutine completes.
					wg.Done()
					// 2. Release the worker for next job
					registerChan <- worker
				}(wg, i)
			}
		}
		// Wait for all workers to complete.
		wg.Wait()
	}

	fmt.Printf("Schedule: %v done\n", phase)
}
