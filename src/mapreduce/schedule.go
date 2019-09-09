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
		taskCh chan int
		wg     sync.WaitGroup
	)
	taskCh = make(chan int, ntasks)
	// init task channel
	for i := 0; i < ntasks; i++ {
		taskCh <- i
	}
	wg.Add(ntasks)

	// schedule
	go func() {
		for {
			// for each task
			task := <-taskCh
			// Launch a goroutine to do map/reduce.
			go func(i int) {
				for {
					// get a worker
					worker := <-registerChan
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
						fmt.Printf("Worker: RPC %s DoTask %d is done\n", worker, i)
						// Decrement the counter when the goroutine completes.
						wg.Done()
						// Release the worker for next job
						registerChan <- worker
						break
					}
				}
			}(task)
		}
	}()
	// Wait for all workers to complete.
	wg.Wait()

	fmt.Printf("Schedule: %v done\n", phase)
}
