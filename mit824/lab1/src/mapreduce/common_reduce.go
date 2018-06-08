package mapreduce

import (
	"encoding/json"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
	var kvSlice []KeyValue

	// loop over all map tasks to get all intermediate result for this reduceTask
	for i := 0; i < nMap; i++ {
		var intermediateFname string = reduceName(jobName, i, reduceTask)
		f, _ := os.Open(intermediateFname)
		dec := json.NewDecoder(f)
		// go through the whole file to get all kvs
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvSlice = append(kvSlice, kv)
		}
		f.Close()
	}
	sort.Slice(kvSlice, func(i, j int) bool {
		return kvSlice[i].Key < kvSlice[j].Key
	})

	outF, _ := os.Create(outFile)
	outEco := json.NewEncoder(outF)

	if len(kvSlice) == 0 {
		outF.Close()
		return
	}

	var kvSliceLen int = len(kvSlice)
	kvSlice = append(kvSlice, KeyValue{kvSlice[kvSliceLen-1].Key + "$$", ""})

	var keyNow string = kvSlice[0].Key
	var startIdx int = 0
	for i := 0; i < len(kvSlice); i++ {
		if keyNow != kvSlice[i].Key {
			strSlice := make([]string, i-startIdx)
			for j := startIdx; j < i; j++ {
				strSlice[j-startIdx] = kvSlice[j].Value
			}
			reduceByKeyAns := reduceF(keyNow, strSlice)
			outKV := KeyValue{keyNow, reduceByKeyAns}
			outEco.Encode(&outKV)
			keyNow = kvSlice[i].Key
			startIdx = i
		}
	}

	outF.Close()

}
