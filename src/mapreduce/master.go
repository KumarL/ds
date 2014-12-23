package mapreduce

import "container/list"
import "fmt"

type WorkerInfo struct {
    address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) JobNameToSchedule() JobType {
    mr.nMapScheduled++
    if mr.nMapScheduled < mr.nMap {
        DPrintf("Scheduling map: %d\n", mr.nMapScheduled)
        return "Map"
    } else {
        DPrintf("Returning empty string as job type\n")
        return "" 
    }
}

func (mr *MapReduce) RegisterNewWorker(workerAddress string) {
    DPrintf("Registering new worker: %s\n", workerAddress)
    w := WorkerInfo{workerAddress}
    mr.Workers[workerAddress] = &w
    //mr.AvailableWorkers.PushBack(&w)
    go func() {
        DPrintf("Sending the signal for new worker\n")
        mr.newWorkerAvailable <- workerAddress
    }()
    DPrintf("Exiting RegisterNewWorker\n")
}

func (mr* MapReduce) ScheduleNextPendingJob(workerAddress string) {
    // Find the next job type
    // Execute it over rpc on the input address
    // make the workerAddress unavailable for any further job
    
    // make the worker unavailable for any further job
    delete(mr.Workers, workerAddress)     
    
    DPrintf("Scheduling the next pending job\n")
    job := mr.JobNameToSchedule()
    if job == "" {
        go func() { 
            DPrintf("Sending AnnounceFinished channel\n")
            mr.AnnounceFinished <- true
        }()
    } else {
        args := &DoJobArgs{mr.file, job, mr.nMapScheduled, mr.nReduce}
        var jobReply DoJobReply
        DPrintf("Calling worker\n")
        call(workerAddress, "Worker.DoJob", args, &jobReply)
        DPrintf("Finished calling worker\n")
    
        // the job finished executing.
        // send a signal that this job is available for execution again
        mr.RegisterNewWorker(workerAddress)
    }
} 
    
func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
        allWorkFinished := false
	for !allWorkFinished {
	    select {
	    case newWorkerAddress := <-mr.registerChannel:
                go mr.RegisterNewWorker(newWorkerAddress)
            case availableWorkerAddress := <-mr.newWorkerAvailable:
                go mr.ScheduleNextPendingJob(availableWorkerAddress)
            case allWorkFinished = <-mr.AnnounceFinished:
                DPrintf("Received AnnounceFinished channel\n")
	    }
            DPrintf("Finished another iteration\n")
	}

	return mr.KillWorkers()
}
