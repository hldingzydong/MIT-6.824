package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "time"
import "encoding/json"
import "strconv"
import "io/ioutil"
import "strings"
import "sort"

//
// Map functions return a slice of KeyValue.
//
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

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// request master for a task
	for {
		var gtArgs GTArgs
		var gtRV GTRV
		res := call("Master.GetTask", &gtArgs, &gtRV) 
		if res == false { // 没有task了,那退出吧
			os.Exit(1)
		}

		if gtRV.IsTaskExist == true {
			if gtRV.IsMapTask == true { // 执行map task
				kva := mapf(gtRV.Filename, gtRV.Content)

				filedp := make(map[string]*os.File)
				// 通过ihash(),partition到不同文件中
				for _, kv := range kva {
					reduce_task_id := ihash(kv.Key)%10
					// 根据reduce_task_id，此时可以确定filename,然后看此时系统中该file是否存在，不存在则创建临时文件
					filename := getInterTempFileName(gtRV.Map_task_id, reduce_task_id)

					var err error
					ofile, ok := filedp[filename]
					if ok == false {
						// 此时不存在该文件，创建临时文件
						curDir, _ := os.Getwd()
						ofile, err = ioutil.TempFile(curDir, "simple")
						if err != nil {
							log.Fatal(err)
						}
						os.Rename(ofile.Name(), filename)
						filedp[filename] = ofile
					}
					
					enc := json.NewEncoder(ofile)
					err = enc.Encode(&kv)
				}
				// partition 完成, 告知master done
				var ndArgs NDArgs
				var ndRV NDRV
				ndArgs.IsSuccess = true
				ndArgs.IsMapTask = true
				ndArgs.Map_task_id = gtRV.Map_task_id
				ndArgs.Filename = gtRV.Filename

				call("Master.NotifyDone", &ndArgs, &ndRV)

				// close filedp
				for _, file := range filedp {
					file.Close()
				}

			}else{	// 执行reduce task
				// 当读取每一个temp file后记得删除它
				// file_index, _ := strconv.Atoi(gtRV.Filename)
				// find所有以file_index为后缀的temp file
				files, err := ioutil.ReadDir(".")
				if err != nil {
					log.Fatal(err)
				}

				// 从文件中读取所有的kv
				var kva []KeyValue
				// var close_filename = make(map[string]bool)
				for _, file := range files {
					if strings.Contains(file.Name(), gtRV.Filename + ".txt") {
						ofile, err := os.Open(file.Name())
						if err != nil {
							log.Fatal(err)
						}
						
						dec := json.NewDecoder(ofile)
  						for {
    						var kv KeyValue
    						if err := dec.Decode(&kv); err != nil {
      							break
    						}
    						kva = append(kva, kv)
  						}
  						ofile.Close()
  						
  						// 添加临时文件的名字至 close_filename中
  						//close_filename[file.Name()] = true
					}
				}
				// 先进行排序
				sort.Sort(ByKey(kva))
				// 创建最终生成的fiel
				nfilename := "mr-out-" + gtRV.Filename
				nfilename += ".txt"
				nfile, _ := os.Create(nfilename)

				// 对kva进行reduce
  				i := 0
				for i < len(kva) {
					j := i + 1
					for j < len(kva) && kva[j].Key == kva[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, kva[k].Value)
					}
					output := reducef(kva[i].Key, values)

					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(nfile, "%v %v\n", kva[i].Key, output)

					i = j
				}
				nfile.Close()

				// 告知master reduce task完成
				var ndArgs NDArgs
				var ndRV NDRV
				ndArgs.IsSuccess = true
				ndArgs.IsMapTask = false
				ndArgs.Reduce_task_id = gtRV.Reduce_task_id
				ndArgs.Filename = gtRV.Filename

				call("Master.NotifyDone", &ndArgs, &ndRV)

				for _, file := range files {
					if strings.Contains(file.Name(), gtRV.Filename + ".txt") {
						if strings.Contains(file.Name(), "mr-out") {
							continue
						}
						os.Remove(file.Name())
					}
				}
			}
		}

		time.Sleep(time.Second)
	}
}


//
// func: get intermidate temp file name
//
func getInterTempFileName(map_task_id int, reduce_task_id int) string {
	var res = "mr-"
	res += strconv.Itoa(map_task_id)
	res += "-"
	res += strconv.Itoa(reduce_task_id)
	res += ".txt"
	return res
}


//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
