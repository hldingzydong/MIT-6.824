package mr

import "fmt"
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "io/ioutil"
import "errors"
import "strconv"
import "time"

type Master struct {
	task_id_source int     			// 每当有一个新的task时,用它来生成task_id 

	map_tasks map[string]int 		// 记录filename正在被哪个map_task_id进行map
	map_tasks_done map[string]bool  // 记录对filename的map任务是否已经完成

	reduce_tasks map[int]int    	// 记录mr-out-{index}正在被哪个reduce_task_id进行reduce
	reduce_tasks_done map[int]bool  // 记录mr-out-{index}的reduce任务是否已经完成

	mu sync.Mutex  					// 互斥锁 因为只有map阶段结束后才可以进行reduce,所以一把锁就足够了
}

//
// master init fucntion
//
func (m *Master) master_init(files []string, nReduce int) {
	m.task_id_source = 0
	m.map_tasks = make(map[string]int)
	m.map_tasks_done = make(map[string]bool)
	for _, filename := range files {
		m.map_tasks[filename] = -1
		m.map_tasks_done[filename] = false
	}
	
	m.reduce_tasks = make(map[int]int)
	m.reduce_tasks_done = make(map[int]bool)
	for i := 0; i < nReduce; i++ {
		m.reduce_tasks[i] = -1
		m.reduce_tasks_done[i] = false
	}
}


//
// worker call for get a map/reduce task
//
func (m *Master) GetTask(args *GTArgs,  reply *GTRV) error {
	// 首先check是否所有的任务已经完成
	if(m.Done()) {
		return errors.New("mr/master.go: MapReduce Task Done, Please Exit")
	}

	if(m.map_done() == false) { // 此时map阶段还未结束,应当给予一个map task
		m.mu.Lock()
		defer m.mu.Unlock()

		for filename := range m.map_tasks {
			if m.map_tasks[filename] == -1 {
				reply.IsTaskExist = true
				reply.IsMapTask = true

				m.task_id_source++
				reply.Map_task_id = m.task_id_source

				m.map_tasks[filename] = reply.Map_task_id
				reply.Filename = filename

				// read file's content
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
					return err
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
					return err
				}
				file.Close()

				reply.Content = string(content)
				//fmt.Printf("mr/master.go: give a map task %v for mapping %s \n", reply.Map_task_id, reply.Filename)
				return nil
			}
		}
	}else{ // 此时map阶段已经结束,应当给予一个reduce任务
		m.mu.Lock()
		defer m.mu.Unlock()
		
		for index := range m.reduce_tasks {
			if m.reduce_tasks[index] == -1 {
				reply.IsTaskExist = true
				reply.IsMapTask = false

				m.task_id_source++
				reply.Reduce_task_id = m.task_id_source

				m.reduce_tasks[index] = reply.Reduce_task_id
				// 这里reply给worker的filename为要处理的file的后缀,即告知worker当遇到一个reduce的task时
				// 应当读取reply.filename 然后在当前目录下查找对应的后缀的文件
				reply.Filename = strconv.Itoa(index)	
				//fmt.Printf("mr/master.go: give a reduce task %v for reducing %s \n", reply.Reduce_task_id, reply.Filename)
				return nil
			}
		}

	}
	reply.IsTaskExist = false
	return nil
}


//
// when worker finished task call this method
//
func (m *Master) NotifyDone(args *NDArgs, reply *NDRV) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if args.IsMapTask == true {	// 一个map task完成了
		if args.IsSuccess {
			m.map_tasks_done[args.Filename] = true
			//fmt.Printf("mr/master.go: Map task %v from %s done\n", args.Map_task_id, args.Filename)
		}else{
			m.map_tasks[args.Filename] = -1
			//fmt.Printf("mr/master.go: Map task %v from %s failed\n", args.Map_task_id, args.Filename)
		}
	}else{	// 一个reduce task完成了
		file_index, err := strconv.Atoi(args.Filename)
		if(err != nil) {
			fmt.Println(err)
			return err
		}
		if args.IsSuccess {
			m.reduce_tasks_done[file_index] = true
			//fmt.Printf("mr/master.go: Reduce task %v from %s done\n", args.Reduce_task_id, args.Filename)
		}else{
			m.reduce_tasks[file_index] = -1
			//fmt.Printf("mr/master.go: Reduce task %v from %s failed\n", args.Map_task_id, args.Filename)
		}
	}

	return nil
}


//
// master map phase is done 
//
func (m *Master) map_done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	for filename := range m.map_tasks_done {
		if m.map_tasks_done[filename] == false {
			return false
		}
	}

	return true
}


//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i := range m.reduce_tasks_done {
		if m.reduce_tasks_done[i] == false {
			return false
		}
	}

	return true
}


//
// func: timer - every 10 senconds check if there is any task died
//
func (m *Master) Timer() {
	for{
		time.Sleep(10 * time.Second)
		//fmt.Println("mr/master.go: ------ checking any task died ------")
		// check是否有task在执行中，若是则放弃该task
		m.mu.Lock()
		
		// check map tasks done 
		for filename := range m.map_tasks {
			if m.map_tasks[filename] != -1 {
				if m.map_tasks_done[filename] == false {
					//fmt.Printf("mr/master.go: ------ map %s file failed ------\n", filename)
					m.map_tasks[filename] = -1
				}
			}
		}
		// check reduce tasks done
		for i := range m.reduce_tasks {
			if m.reduce_tasks[i] != -1 {
				if m.reduce_tasks_done[i] == false {
					//fmt.Printf("mr/master.go: ------ reduce %v task failed ------\n", i)
					m.reduce_tasks[i] = -1
				}
			}
		}

		m.mu.Unlock()
	}
}


//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// init master
	m.master_init(files, nReduce);

	m.server()

	go m.Timer()

	return &m
}
