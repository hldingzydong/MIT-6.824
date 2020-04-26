package mr

import "fmt"
import "testing"

func TestGetTask(t *testing.T) {
	fmt.Println("----------------------")
	fmt.Println("Begin To Test Master")
	fmt.Println("----------------------")
	var gtArgs GTArgs
	var gtRV GTRV
	var files [1]string
	files[0] = "pg-grimm.txt"

	// init master
	m := Master{}
	m.master_init(files[0:], 1)
	//
	// call first get task
	//
	err := m.GetTask(&gtArgs, &gtRV)
	if err == nil {
		fmt.Printf("mr/master_test.go: gtRV.IsTaskExist = %v, gtRV.IsMapTask = %v, gtRV.Map_task_id = %v, gtRV.Filename = %s\n", 
			gtRV.IsTaskExist, gtRV.IsMapTask, gtRV.Map_task_id, gtRV.Filename)
	}else{
		fmt.Println(err)
	}
	//
	// call second get task
	//
	var gtArgs2 GTArgs
	var gtRV2 GTRV
	err = m.GetTask(&gtArgs2, &gtRV2)
	if err == nil {
		fmt.Printf("mr/master_test.go: gtRV2.IsTaskExist = %v, gtRV2.IsMapTask = %v, gtRV2.Map_task_id = %v, gtRV2.Filename = %s\n", 
			gtRV2.IsTaskExist, gtRV2.IsMapTask, gtRV2.Map_task_id, gtRV2.Filename)
	}else{
		fmt.Println(err)
	}
	//
	// call notify done
	//
	var args NDArgs
	var reply NDRV

	args.IsSuccess = true
	args.IsMapTask = true
	args.Map_task_id = gtRV.Map_task_id
	args.Filename = gtRV.Filename
	err = m.NotifyDone(&args, &reply)
	if err == nil {
		fmt.Printf("mr/master_test.go: notify master task %d done\n", args.Map_task_id)
	}else{
		fmt.Println(err)
	}
	//
	// call reduce task
	//
	err = m.GetTask(&gtArgs2, &gtRV2)
	if err == nil {
		fmt.Printf("mr/master_test.go: gtRV2.IsTaskExist = %v, gtRV2.IsMapTask = %v, gtRV2.Reduce_task_id = %v, gtRV2.Filename = %s\n", 
			gtRV2.IsTaskExist, gtRV2.IsMapTask, gtRV2.Reduce_task_id, gtRV2.Filename)
	}else{
		fmt.Println(err)
	}
	//
	// call notify done
	// 
	args.IsSuccess = true
	args.IsMapTask = false
	args.Reduce_task_id = gtRV2.Reduce_task_id
	args.Filename = gtRV2.Filename
	err = m.NotifyDone(&args, &reply)
	if err == nil {
		fmt.Printf("mr/master_test.go: notify master task %d done\n", args.Reduce_task_id)
	}else{
		fmt.Println(err)
	}

	isDone := m.Done()
	if(isDone) {
		fmt.Println("MapReduce Done")
	}

	// call task
	err = m.GetTask(&gtArgs2, &gtRV2)
	if err == nil {
		fmt.Printf("mr/master_test.go: gtRV2.IsTaskExist = %v, gtRV2.IsMapTask = %v, gtRV2.Reduce_task_id = %v, gtRV2.Filename = %s\n", 
			gtRV2.IsTaskExist, gtRV2.IsMapTask, gtRV2.Reduce_task_id, gtRV2.Filename)
	}else{
		fmt.Println(err)
	}

	fmt.Println("----------------------")
	fmt.Println("End To Test Master")
	fmt.Println("----------------------")
}