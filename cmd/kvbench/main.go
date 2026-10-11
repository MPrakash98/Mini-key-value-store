// kvbench is a load generator for the kvstore.
//
// Flags:
//
//	-dir        data directory
//	-n          number of operations (default 10000)
//	-value-size value size in bytes (default 256)
//	-sync       always|interval|never (default always)
//	-workload   fillseq|fillrandom|readrandom|mixed (default fillrandom)
//	-threads    number of goroutines (default 1)
//	-csv        path to write CSV results
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"kvstore/internal/db"
)

func main() {
	dir := flag.String("dir", "bench-data", "data directory")
	n := flag.Int("n", 10000, "number of operations")
	valueSize := flag.Int("value-size", 256, "value size in bytes")
	syncFlag := flag.String("sync", "always", "sync policy: always|interval|never")
	workload := flag.String("workload", "fillrandom", "fillseq|fillrandom|readrandom|mixed")
	threads := flag.Int("threads", 1, "number of goroutines")
	csvPath := flag.String("csv", "", "write results to this CSV file")
	flag.Parse()

	var policy db.SyncPolicy
	switch *syncFlag {
	case "always":
		policy = db.SyncAlways
	case "interval":
		policy = db.SyncInterval
	case "never":
		policy = db.SyncNever
	default:
		fmt.Fprintf(os.Stderr, "unknown sync: %s\n", *syncFlag)
		os.Exit(1)
	}

	store, err := db.Open(db.Options{
		Dir:          *dir,
		SyncPolicy:   policy,
		UseHintFiles: true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "open: %v\n", err)
		os.Exit(1)
	}
	defer store.Close()

	value := make([]byte, *valueSize)
	for i := range value {
		value[i] = byte(i % 256)
	}

	// For readrandom we pre-populate keys.
	if *workload == "readrandom" {
		fmt.Println("pre-populating for readrandom...")
		for i := 0; i < *n; i++ {
			store.Put(keyFor(*workload, i, *n), value)
		}
	}

	latencies := make([]time.Duration, 0, *n)
	var mu sync.Mutex

	perThread := *n / *threads
	var wg sync.WaitGroup
	start := time.Now()

	for g := 0; g < *threads; g++ {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(g)))
			local := make([]time.Duration, 0, perThread)

			for i := 0; i < perThread; i++ {
				globalI := g*perThread + i
				opStart := time.Now()

				switch *workload {
				case "fillseq":
					store.Put(keyFor("fillseq", globalI, *n), value)
				case "fillrandom":
					store.Put(keyFor("fillrandom", rng.Intn(*n), *n), value)
				case "readrandom":
					store.Get(keyFor("readrandom", rng.Intn(*n), *n))
				case "mixed":
					if rng.Intn(5) < 4 { // 80% read
						store.Get(keyFor("mixed", rng.Intn(*n), *n))
					} else {
						store.Put(keyFor("mixed", rng.Intn(*n), *n), value)
					}
				}

				local = append(local, time.Since(opStart))
			}

			mu.Lock()
			latencies = append(latencies, local...)
			mu.Unlock()
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)

	// Compute statistics.
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	total := len(latencies)
	opsSec := float64(total) / elapsed.Seconds()
	mbSec := opsSec * float64(*valueSize) / (1024 * 1024)

	p := func(pct float64) time.Duration {
		idx := int(float64(total)*pct/100) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= total {
			idx = total - 1
		}
		return latencies[idx]
	}

	fmt.Printf("\n=== kvbench results ===\n")
	fmt.Printf("workload:   %s\n", *workload)
	fmt.Printf("sync:       %s\n", *syncFlag)
	fmt.Printf("threads:    %d\n", *threads)
	fmt.Printf("ops:        %d\n", total)
	fmt.Printf("value-size: %d B\n", *valueSize)
	fmt.Printf("elapsed:    %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("ops/sec:    %.0f\n", opsSec)
	fmt.Printf("MB/s:       %.2f\n", mbSec)
	fmt.Printf("p50:        %s\n", p(50))
	fmt.Printf("p95:        %s\n", p(95))
	fmt.Printf("p99:        %s\n", p(99))
	fmt.Printf("max:        %s\n", latencies[total-1])

	if *csvPath != "" {
		writeCSV(*csvPath, *workload, *syncFlag, *threads, total, *valueSize,
			elapsed, opsSec, mbSec, p(50), p(95), p(99), latencies[total-1])
	}
}

func keyFor(workload string, i, n int) []byte {
	_ = n
	return []byte(fmt.Sprintf("%s-key-%08d", workload, i))
}

func writeCSV(path, workload, sync string, threads, ops, valSize int,
	elapsed time.Duration, opsSec, mbSec float64,
	p50, p95, p99, pmax time.Duration) {

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "csv open: %v\n", err)
		return
	}
	defer f.Close()

	w := csv.NewWriter(f)
	w.Write([]string{
		workload, sync, strconv.Itoa(threads), strconv.Itoa(ops),
		strconv.Itoa(valSize), elapsed.String(),
		fmt.Sprintf("%.0f", opsSec), fmt.Sprintf("%.2f", mbSec),
		p50.String(), p95.String(), p99.String(), pmax.String(),
	})
	w.Flush()
}
