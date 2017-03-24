package mapreduce

import "container/list"
import "fmt"


type WorkerInfo struct {
	address string
	// You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
// MapReduce 中记录了 master 下面的 workers.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		// 使用 common.go 中的 call 发送关闭命令给 workers
		// 调用 worder.go 中的 Shutdown 方法
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// 使用 Channel 来存储协程任务
	MapChan := make(chan int, mr.nMap)
	ReduceChan := make(chan int, mr.nReduce)

	rpc_map := func(worker string, job_number int) bool {
		var jobArgs DoJobArgs
		var reply DoJobReply
		jobArgs.File = mr.file
		jobArgs.Operation = Map
		jobArgs.JobNumber = job_number
		jobArgs.NumOtherPhase = mr.nReduce
		var ret = call(worker, "Worker.DoJob", jobArgs, &reply)
		return ret
	}

	rpc_reduce := func(worker string, job_number int) bool {
		var jobArgs DoJobArgs
		var reply DoJobReply
		jobArgs.File = mr.file
		jobArgs.Operation = Reduce
		jobArgs.JobNumber = job_number
		jobArgs.NumOtherPhase = mr.nMap
		var ret = call(worker, "Worker.DoJob", jobArgs, &reply)
		return ret
	}

	// mr 中有 nMap 个 Map 任务，执行 Map 任务
	for MapIndex := 0; MapIndex < mr.nMap; MapIndex++ {
		go func(job_number int) {
			for {
				var worker string
				var ok bool = false
				select {
					case worker = <- mr.idleChannel:
						ok = rpc_map(worker, job_number)
					case worker = <- mr.registerChannel: // 当 worker.go 在频道中注册任务时
						ok = rpc_map(worker, job_number)
				}
				if (ok) {
					MapChan <- job_number
					mr.idleChannel <- worker // 用 idleChannel 保存闲置的 worder
					return
				}
			}
		}(MapIndex)
	}

	// 清空 MapChan
	for MapIndex := 0; MapIndex < mr.nMap; MapIndex++ {
		<- MapChan
	}

	fmt.Println("Map is Done")

	// 执行 Reduce 任务
	for ReduceIndex := 0; ReduceIndex < mr.nReduce; ReduceIndex++ {
		go func(job_number int) {
			for {
				var worker string
				var ok bool = false
				select {
					case worker = <- mr.idleChannel:
						ok = rpc_reduce(worker, job_number)
					case worker = <- mr.registerChannel: // 当 worker.go 在频道中注册任务时
						ok = rpc_reduce(worker, job_number)
				}
				if (ok) {
					ReduceChan <- job_number
					mr.idleChannel <- worker // 用 idleChannel 保存闲置的 worder
					return
				}
			}
		}(ReduceIndex)
	}

	// 清空 ReduceChan
	for ReduceIndex := 0; ReduceIndex < mr.nReduce; ReduceIndex++ {
		<- ReduceChan
	}

	fmt.Println("Reduce is Done")
	return mr.KillWorkers()
}
