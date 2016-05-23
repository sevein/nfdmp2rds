package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"time"

	"github.com/garyburd/redigo/redis"

	"github.com/sevein/nfdmp2rds/entry"
)

var (
	logger = log.New(os.Stderr, "", 0)
	pool   *redis.Pool
)

var (
	redisServer   = flag.String("redisServer", ":6379", "Redis server")
	redisPassword = flag.String("redisPassword", "", "Redis password")
	batchSize     = flag.Int("bsize", 5, "Batch size")
	hostname      = flag.String("hostname", "localhost", "Given hostname")
	cpuprofile    = flag.String("cpuprofile", "", "Write CPU profile to file")
	flush         = flag.Bool("flush", false, "Delete key ")
	workers       = flag.Int("workers", 2, "Number of workers")
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
	redisListKey := args[0]
	input := args[1]

	// Set hostname
	if *hostname != "" {
		entry.Hostname = *hostname
	}

	// Enable CPU profiling
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			logger.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	// Create pool of redis connections. We are using just one connection for
	// now but I am hoping to eventually support multiple workers.
	pool = newPool(*redisServer, *redisPassword)
	defer pool.Close()
	conn := pool.Get()
	defer conn.Close()

	// Open file or pipe
	file, err := openFile(input)
	if err != nil {
		logger.Fatalf("Error encountered while reading input: %s.", err)
	}

	// Delete existing list
	if *flush {
		if err := delKey(pool, redisListKey); err != nil {
			logger.Fatalf("Key \"%s\" could not be deleted: %s.", redisListKey, err)
		}
		logger.Printf("Key \"%s\" has been deleted.", redisListKey)
	}

	i := -1
	success := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		e, err := entry.NewNfdumpEntry(scanner.Text())
		if err != nil {
			fmt.Println(err)
			continue
		}
		j, err := e.MarshalJSON()
		if err != nil {
			continue
		}
		if err := conn.Send("RPUSH", redisListKey, string(j)); err != nil {
			logger.Println("Send() failed", err)
			continue
		}

		i++
		success++
		if i < *batchSize {
			if err := conn.Flush(); err != nil {
				logger.Println("Flush() failed", err)
			}
			i = -1
			if *verbose {
				logger.Println("Flushing...")
			}
		}
	}
	if err := scanner.Err(); err != nil {
		logger.Println("Error encountered while reading file", err)
	}

	// Say good-bye!
	logger.Println("Done! nfdmp2rds finished successfully.")
	count, err := redis.Int64(conn.Do("LLEN", redisListKey))
	if err != nil {
		logger.Fatalln("LLEN failed:", err)
	}
	logger.Printf("List \"%s\" has now %d entries! This import tried to introduce %d entries.\n", redisListKey, count, success)
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

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: nfdmp2rds [options] redisListKey file\n")
	fmt.Fprintf(os.Stderr, "(redisListKey and file mandatory)\n\n")
	fmt.Fprintf(os.Stderr, "Flags (options):\n")
	flag.PrintDefaults()
	os.Exit(2)
}
