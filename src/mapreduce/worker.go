package mapreduce

import "fmt"
import "os"
import "log"
import "net/rpc"
import "net"
import "container/list"

// Worker is a server waiting for DoJob or Shutdown RPCs

type Worker struct {
	name   string
	Reduce func(string, *list.List) string  // 匿名函数定义
	Map    func(string) *list.List
	nRPC   int
	nJobs  int
	l      net.Listener
}

// The master sent us a job
// DoJobArgs 等参数都在 common.go 中
func (wk *Worker) DoJob(arg *DoJobArgs, res *DoJobReply) error {
	fmt.Printf("Dojob %s job %d file %s operation %v N %d\n",
		wk.name, arg.JobNumber, arg.File, arg.Operation,
		arg.NumOtherPhase)
	switch arg.Operation {
	case Map:
		// 调用 mapreduce 中的 DoMap 和 DoReduce
		DoMap(arg.JobNumber, arg.File, arg.NumOtherPhase, wk.Map)
	case Reduce:
		DoReduce(arg.JobNumber, arg.File, arg.NumOtherPhase, wk.Reduce)
	}
	res.OK = true
	return nil
}

// The master is telling us to shutdown. Report the number of Jobs we
// have processed.
func (wk *Worker) Shutdown(args *ShutdownArgs, res *ShutdownReply) error {
	DPrintf("Shutdown %s\n", wk.name)
	res.Njobs = wk.nJobs
	res.OK = true
	wk.nRPC = 1 // OK, because the same thread reads nRPC
	wk.nJobs--  // Don't count the shutdown RPC
	return nil
}

// Tell the master we exist and ready to work
func Register(master string, me string) {
	args := &RegisterArgs{}
	args.Worker = me
	var reply RegisterReply
	// 调用 mapreduce.go 中的 Register 方法，往 mapreduce 中注册 worker 任务
	ok := call(master, "MapReduce.Register", args, &reply)
	if ok == false {
		fmt.Printf("Register: RPC %s register error\n", master)
	}
}

// Set up a connection with the master, register with the master,
// and wait for jobs from the master
func RunWorker(MasterAddress string, me string,
	MapFunc func(string) *list.List,
	ReduceFunc func(string, *list.List) string, nRPC int) {
	DPrintf("RunWorker %s\n", me)
	wk := new(Worker)
	wk.name = me
	wk.Map = MapFunc
	wk.Reduce = ReduceFunc
	wk.nRPC = nRPC

	// 新建 rpc 的 server，监听请求
	rpcs := rpc.NewServer()
	rpcs.Register(wk)
	os.Remove(me) // only needed for "unix" 删除文件
	l, e := net.Listen("unix", me) // 使用 UNIX Socket 通信
	if e != nil {
		log.Fatal("RunWorker: worker ", me, " error: ", e)
	}
	wk.l = l

	// 向 master 的 mapreduce 发起 RPC 请求注册
	Register(MasterAddress, me)

	// DON'T MODIFY CODE BELOW
	for wk.nRPC != 0 {
		conn, err := wk.l.Accept()
		if err == nil {
			wk.nRPC -= 1
			go rpcs.ServeConn(conn) // 使用协程，调用会阻塞，服务该连接直到客户端挂起
			wk.nJobs += 1
		} else {
			break
		}
	}
	wk.l.Close()
	DPrintf("RunWorker %s exit\n", me)
}
