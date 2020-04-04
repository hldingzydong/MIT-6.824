package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

//
// get task args
//
type GTArgs struct {

}

// get task return value
type GTRV struct {
	IsTaskExist bool   // true代表有task false代表没有task但是整个MapReduce还没有结束，一会再来看看有没有task吧
	IsMapTask bool
	Map_task_id int   	// 0~7 因为仅八个txt文件 
	Reduce_task_id int   // 0～9 因为nReduce == 10
	Filename string // 其实没啥用
	Content string
}


//
// notify done args
//
type NDArgs struct {
	IsSuccess bool
	IsMapTask bool
	Map_task_id int
	Reduce_task_id int
	Filename string
}

// notify done return value
type NDRV struct {

}



// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
