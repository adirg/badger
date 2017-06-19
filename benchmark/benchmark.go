package main

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"sort"
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

	latency := make([]int64, bench.opt.Records)
	var wg sync.WaitGroup
	start := time.Now()
	keysPerThread := bench.opt.Records / bench.opt.LoadThreads
	for i := 0; i < bench.opt.LoadThreads; i++ {
		wg.Add(1)
		go func(kv *badger.KV, startKey int) {
			value := make([]byte, bench.opt.ValueSize)
			rand.Read(value)
			for j := startKey; j < startKey+keysPerThread; j++ {
				key := fmt.Sprint(j)
				start := time.Now()
				kv.Set([]byte(key), value)
				latency[j] = time.Since(start).Nanoseconds()

			}
			wg.Done()
		}(bench.kv, i*keysPerThread)
	}
	wg.Wait()
	elapsed := time.Since(start)

	printStats("load", bench.opt.Records, elapsed, latency)
}

func (bench *Benchmark) Run() {
	fmt.Println("Running...")

	latency := make([]int64, bench.opt.Records)
	var wg sync.WaitGroup
	start := time.Now()
	keysPerThread := bench.opt.Records / bench.opt.RunThreads
	for i := 0; i < bench.opt.RunThreads; i++ {
		wg.Add(1)
		go func(kv *badger.KV, startKey, idx int) {
			for j := startKey; j < startKey+keysPerThread; j++ {
				key := fmt.Sprint(startKey + rand.Intn(keysPerThread))
				var item badger.KVItem
				item.Hint = idx
				start := time.Now()
				kv.Get([]byte(key), &item)
				latency[j] = time.Since(start).Nanoseconds()
			}
			wg.Done()
		}(bench.kv, i*keysPerThread, i)
	}
	wg.Wait()
	elapsed := time.Since(start)
	printStats("run", bench.opt.Records, elapsed, latency)
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

func printStats(stage string, numOps int, period time.Duration, latency []int64) {
	fmt.Printf("===== %s stats\n", stage)
	printIOPS(numOps, period)
	printLatencyStats(latency)
	fmt.Printf("=====\n")
}

func printIOPS(numOps int, period time.Duration) {
	fmt.Printf("iops = %f\n", float64(numOps)/period.Seconds())
}

func printLatencyStats(latency []int64) {
	n := len(latency)
	sort.Slice(latency, func(i, j int) bool { return latency[i] < latency[j] })

	min := latency[0]
	max := latency[n-1]

	sum := int64(0)
	for i := range latency {
		sum += latency[i]
	}
	avg := float64(sum) / float64(n)

	fmt.Println("latency stats:")
	fmt.Println("max: ", max)
	fmt.Println("min: ", min)
	fmt.Println("avg: ", avg)

	p99idx := int(math.Floor(0.99 * float64(n)))
	fmt.Println("p99idx: ", p99idx)
	fmt.Println("p99 latency: ", latency[p99idx])
}
