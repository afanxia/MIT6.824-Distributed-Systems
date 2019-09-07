package mapreduce

import (
	"encoding/json"
	"fmt"
	"io"
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
	var (
		inFile string
		kv     KeyValue
		kvs    []KeyValue
		key    string
		values []string
	)
	// read reduceTask'th file from each nMap
	for m := 0; m < nMap; m++ {
		inFile = reduceName(jobName, m, reduceTask)
		f, err := os.Open(inFile)
		if err != nil {
			f.Close()
			fmt.Println("doReduce error when reading input: ", err)
			return
		}
		dec := json.NewDecoder(f)
		for {
			if err := dec.Decode(&kv); err == io.EOF {
				break
			} else if err != nil {
				fmt.Println("doReduce error when decoding: ", err)
				f.Close()
				return
			}
			kvs = append(kvs, kv)
		}
		f.Close()
	}

	// sort the kvs by Key
	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].Key < kvs[j].Key
	})

	// create output file
	of, err := os.Create(outFile)
	if err != nil {
		fmt.Println("Cannot create reduce output:", outFile)
		return
	}

	// reduce and dump to output
	enc := json.NewEncoder(of)
	key = ""
	for j, kv := range kvs {
		if j != 0 && kv.Key != key {
			if len(values) > 0 {
				_ = enc.Encode(KeyValue{key, reduceF(key, values)})
			}
			values = values[:0] // clear values
		}
		key = kv.Key
		values = append(values, kv.Value)
	}
	// don't forget last kv
	_ = enc.Encode(KeyValue{key, reduceF(key, values)}) // need to do it again for the last pairs

	of.Close()

}
