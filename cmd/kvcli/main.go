// kvcli is an interactive REPL for the kvstore.
//
// Commands:
//
//	put <key> <value>  – insert or update
//	get <key>          – retrieve
//	del <key>          – delete
//	compact            – run compaction
//	stats              – print DB statistics
//	sync               – force fsync
//	help               – print usage
//	quit               – exit
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"kvstore/internal/db"
)

func main() {
	dir := flag.String("dir", "data", "data directory")
	syncFlag := flag.String("sync", "always", "sync policy: always|interval|never")
	segMB := flag.Int64("segment-mb", 64, "max segment size in MiB")
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
		fmt.Fprintf(os.Stderr, "unknown sync policy %q; use always|interval|never\n", *syncFlag)
		os.Exit(1)
	}

	store, err := db.Open(db.Options{
		Dir:             *dir,
		SyncPolicy:      policy,
		SegmentMaxBytes: *segMB << 20,
		UseHintFiles:    true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "open: %v\n", err)
		os.Exit(1)
	}
	defer store.Close()

	ri := store.RecoveryInfo()
	fmt.Printf("opened: %d keys, %d segments, recovered in %s",
		ri.Keys, ri.Segments, ri.Duration.Round(time.Millisecond))
	if ri.TruncatedBytes > 0 {
		fmt.Printf(", truncated %d bytes of torn tail", ri.TruncatedBytes)
	}
	fmt.Println()
	fmt.Println("Type 'help' for available commands.")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, " ", 3)
		cmd := strings.ToLower(parts[0])

		switch cmd {
		case "put":
			if len(parts) < 3 {
				fmt.Println("usage: put <key> <value>")
				continue
			}
			if err := store.Put([]byte(parts[1]), []byte(parts[2])); err != nil {
				fmt.Printf("error: %v\n", err)
			} else {
				fmt.Println("OK")
			}

		case "get":
			if len(parts) < 2 {
				fmt.Println("usage: get <key>")
				continue
			}
			val, err := store.Get([]byte(parts[1]))
			if err == db.ErrNotFound {
				fmt.Println("(not found)")
			} else if err != nil {
				fmt.Printf("error: %v\n", err)
			} else {
				fmt.Printf("%s\n", val)
			}

		case "del":
			if len(parts) < 2 {
				fmt.Println("usage: del <key>")
				continue
			}
			if err := store.Delete([]byte(parts[1])); err != nil {
				fmt.Printf("error: %v\n", err)
			} else {
				fmt.Println("OK")
			}

		case "compact":
			start := time.Now()
			if err := store.Compact(); err != nil {
				fmt.Printf("compact error: %v\n", err)
			} else {
				fmt.Printf("compact done in %s\n", time.Since(start).Round(time.Millisecond))
				s := store.Stats()
				fmt.Printf("disk: %d bytes, live: %d bytes\n", s.DiskBytes, s.LiveBytes)
			}

		case "stats":
			s := store.Stats()
			deadRatio := 0.0
			if s.DiskBytes > 0 {
				deadRatio = float64(s.DiskBytes-s.LiveBytes) / float64(s.DiskBytes) * 100
			}
			fmt.Printf("keys:      %d\n", s.Keys)
			fmt.Printf("segments:  %d\n", s.Segments)
			fmt.Printf("disk:      %d bytes\n", s.DiskBytes)
			fmt.Printf("live:      %d bytes\n", s.LiveBytes)
			fmt.Printf("dead:      %.1f%%\n", deadRatio)
			fmt.Printf("next seq:  %d\n", s.NextSeq)

		case "sync":
			if err := store.Sync(); err != nil {
				fmt.Printf("sync error: %v\n", err)
			} else {
				fmt.Println("synced")
			}

		case "help":
			fmt.Println(`Commands:
  put <key> <value>  – insert or update a key
  get <key>          – retrieve a value
  del <key>          – delete a key
  compact            – run garbage collection
  stats              – show storage statistics
  sync               – force fsync to disk
  help               – show this help
  quit               – exit`)

		case "quit", "exit", "q":
			fmt.Println("bye")
			return

		default:
			fmt.Printf("unknown command %q; type 'help'\n", cmd)
		}
	}
}
