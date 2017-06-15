package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/adirg/badger/badger"
)

type Options struct {
	MemcachedServer string
	Dir             string
	Ops             int
	Records         int
	ValueSize       int
	LoadThreads     int
	RunThreads      int
}

type Benchmark struct {
	kv  *badger.KV
	opt Options
}

func NewBenchmark(opt Options) *Benchmark {
	bench := new(Benchmark)

	prepareKVDir(opt.Dir)
	bench.opt = opt
	bench.kv, _ = badger.NewKV(prepareKVOptions(opt.Dir, opt.MemcachedServer, opt.RunThreads+1))

	return bench
}

func (bench *Benchmark) Destroy() {
	bench.kv.Close()
}

func (bench *Benchmark) Load() {
	fmt.Println("Loading...")

	var wg sync.WaitGroup
	start := time.Now()
	keysPerThread := bench.opt.Records / bench.opt.LoadThreads
	for i := 0; i < bench.opt.LoadThreads; i++ {
		wg.Add(1)
		go func(kv *badger.KV, startKey int) {
			value := make([]byte, bench.opt.ValueSize)
			rand.Read(value)
			fmt.Printf("Start Key: %d\n", startKey)
			for j := startKey; j < startKey+keysPerThread; j++ {
				key := fmt.Sprint(j)
				kv.Set([]byte(key), value)
			}
			wg.Done()
		}(bench.kv, i*keysPerThread)
	}
	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("ops = %d\n", bench.opt.Records)
	fmt.Printf("elapsed = %s\n", elapsed)
}

func (bench *Benchmark) Run() {
	fmt.Println("Running...")

	var wg sync.WaitGroup
	start := time.Now()
	keysPerThread := bench.opt.Records / bench.opt.RunThreads
	for i := 0; i < bench.opt.RunThreads; i++ {
		wg.Add(1)
		go func(kv *badger.KV, startKey, idx int) {
			fmt.Printf("Start Key: %d\n", startKey)
			for j := startKey; j < startKey+keysPerThread; j++ {
				key := fmt.Sprint(j)
				var item badger.KVItem
				item.Hint = idx
				kv.Get([]byte(key), &item)
			}
			wg.Done()
		}(bench.kv, i*keysPerThread, i)
	}
	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("ops = %d\n", bench.opt.Records)
	fmt.Printf("elapsed = %s\n", elapsed)
}

func prepareKVDir(dir string) {
	if dirExists, _ := exists(dir); !dirExists {
		err := os.Mkdir(dir, 0777)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func prepareKVOptions(dir, memcachedServer string, memcacheConnections int) *badger.Options {
	opt := new(badger.Options)
	*opt = badger.DefaultOptions

	opt.Dir = dir
	opt.ValueDir = dir
	opt.MemcacheServer = memcachedServer
	opt.MemcacheConnections = memcacheConnections

	return opt
}

// exists returns whether the given file or directory exists or not
func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func max(a, b uint) uint {
	if a < b {
		return b
	}
	return a
}
