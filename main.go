package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"

	"github.com/sevein/nfdmp2rds/entry"
	"github.com/sevein/nfdmp2rds/geoip"
)

var (
	logger       = log.New(os.Stderr, "", 0)
	pool         *redis.Pool
	redisListKey string
)

var (
	redisServer   = flag.String("redisServer", ":6379", "Redis server")
	redisPassword = flag.String("redisPassword", "", "Redis password")
	batchSize     = flag.Int("bsize", 5, "Batch size")
	cpuprofile    = flag.String("cpuprofile", "", "Write CPU profile to file")
	flush         = flag.Bool("flush", false, "Delete key beforehand")
	workers       = flag.Int("workers", 4, "Number of workers")
	verbose       = flag.Bool("v", false, "Verbose mode")
	help          = flag.Bool("h", false, "Print command usage help")
)

func main() {
	flag.Usage = usage
	flag.Parse()
	args := flag.Args()
	if *help || len(args) != 2 {
		flag.Usage()
		os.Exit(1)
	}
	redisListKey = args[0]
	input := args[1]

	// Enable CPU profiling
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			logger.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	// Print geoip version
	if !*entry.NoGeo {
		logger.Printf("Using geographic database: %s", geoip.Info())
	}

	// Create pool of redis connections
	pool = newPool(*redisServer, *redisPassword)
	defer pool.Close()

	// Delete existing list
	if *flush {
		if err := delKey(pool, redisListKey); err != nil {
			logger.Fatalf("Key \"%s\" could not be deleted: %s.", redisListKey, err)
		}
		logger.Printf("Key \"%s\" has been deleted.", redisListKey)
	}

	// Open file or pipe
	file, err := openFile(input)
	if err != nil {
		logger.Fatalf("Error encountered while reading input: %s.", err)
	}
	defer file.Close()

	// Here is where the magic happens!
	if err := process(file); err != nil {
		logger.Fatalln(err)
	}

	// Say good-bye!
	conn := pool.Get()
	defer conn.Close()
	logger.Println("Done! nfdmp2rds finished successfully.")
	count, err := redis.Int64(conn.Do("LLEN", redisListKey))
	if err != nil {
		logger.Fatalln("LLEN failed:", err)
	}
	logger.Printf("List \"%s\" has now %d entries!", redisListKey, count)
}

func newPool(server, password string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			if password != "" {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func openFile(input string) (file *os.File, err error) {
	if input == "-" {
		// Discard stdin if it's connected to a terminal. We want it to be
		// connected to a pipe or a file.
		info, err := os.Stdin.Stat()
		if err != nil {
			return nil, err
		}
		if (info.Mode() & os.ModeCharDevice) == os.ModeCharDevice {
			return nil, errors.New("stdin must be a pipe or a file")
		}
		file = os.Stdin
	} else {
		file, err = os.Open(input)
		if err != nil {
			return nil, err
		}
	}
	return file, nil
}

func delKey(pool *redis.Pool, key string) (err error) {
	conn := pool.Get()
	defer conn.Close()
	_, err = conn.Do("DEL", key)
	return err
}

func process(file *os.File) error {
	done := make(chan struct{})
	defer close(done)

	lines, errc := parser(done, file)
	logger.Println("Parsing has started...")

	// Start a fixed number of goroutines to digest lines.
	errorsc := make(chan error)
	var wg sync.WaitGroup
	wg.Add(*workers)
	for i := 0; i < *workers; i++ {
		go func() {
			digester(done, lines, errorsc)
			wg.Done()
		}()
	}
	logger.Printf("Number of workers running: %d.", *workers)

	go func() {
		wg.Wait()
		close(errorsc)
	}()

	// Drain digester error channel
	for err := range errorsc {
		if err != nil {
			logger.Printf("Error processing entry: %s", err)
		}
	}

	// Check whether the parser failed.
	if err := <-errc; err != nil {
		return err
	}

	return nil
}

// parser starts a goroutine to scan the file and send each line found on the
// string channel. It sends the result of the scan on the error channel. If
// done is closed, parser abandons its work.
func parser(done <-chan struct{}, file *os.File) (<-chan string, <-chan error) {
	lines := make(chan string)
	errc := make(chan error, 1)
	go func() {
		// Close the lines channel after this function returns.
		defer close(lines)

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			select {
			case <-done:
				return
			case lines <- scanner.Text():
			}
		}
		errc <- scanner.Err()
	}()
	return lines, errc
}

func digester(done <-chan struct{}, lines <-chan string, c chan<- error) {
	conn := pool.Get()
	defer conn.Close()

	// digester keeps count of entries pushed so it can be done in batches.
	pushed := 0

	for line := range lines {
		select {
		case c <- rpush(line, &pushed, conn):
		case <-done:
			return
		}
	}
}

func rpush(line string, pushed *int, conn redis.Conn) error {
	e, err := entry.NewNfdumpEntry(line)
	if err != nil {
		return err
	}

	j, err := e.MarshalJSON()
	if err != nil {
		return err
	}

	if err := conn.Send("RPUSH", redisListKey, string(j)); err != nil {
		return err
	}

	if *pushed < *batchSize {
		if err := conn.Flush(); err != nil {
			return err
		}
		*pushed++
		if *verbose {
			logger.Println("Flushing...")
		}
	}

	return nil
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: nfdmp2rds [options] redisListKey file\n")
	fmt.Fprintf(os.Stderr, "(redisListKey and file mandatory)\n\n")
	fmt.Fprintf(os.Stderr, "Flags (options):\n")
	flag.PrintDefaults()
	os.Exit(2)
}
