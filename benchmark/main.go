package main

import (
	"flag"
	"fmt"
	"time"
)

var args Options

func main() {
	fmt.Println("Running Badger...")
	flag.Parse()

	bench := NewBenchmark(args)
	bench.Load()
	time.Sleep(5 * time.Second)
	bench.Run()
	bench.Destroy()
}

func init() {
	flag.StringVar(&args.MemcachedServer, "memcached-server", "", "The memcached server in which to store values")
	flag.StringVar(&args.Dir, "directory", "/tmp/badger", "The directory in which to store values")
	flag.IntVar(&args.Ops, "ops", 1000, "Num of operations in millions")
	flag.IntVar(&args.Records, "records", 1000, "Num of records to load in millions")
	flag.IntVar(&args.ValueSize, "value-size", 1024, "Size of value")
	flag.IntVar(&args.LoadThreads, "load-threads", 10, "Num of loading threads")
	flag.IntVar(&args.RunThreads, "run-threads", 10, "Num of loading threads")
}
