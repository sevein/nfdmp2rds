package main

import (
	"bufio"
	"flag"
	"log"
	"os"
	"time"

	"github.com/garyburd/redigo/redis"

	"github.com/sevein/nfdmp2rds/entry"
)

var (
	redisServer   = flag.String("redisServer", ":6379", "Redis server")
	redisPassword = flag.String("redisPassword", "", "Redis password")
	redisListKey  = flag.String("redisListKey", "foobar", "Key of the list")
	verbose       = flag.Bool("v", false, "Verbose mode")
	batchSize     = flag.Int("bsize", 500, "Batch size")
)

var logger = log.New(os.Stderr, "", 0)

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

func main() {
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		os.Exit(1)
	}

	// Hoping to add concurrent workers with multiple conns later?
	pool := newPool(*redisServer, *redisPassword)
	defer pool.Close()

	// One connection to redis for now
	conn := pool.Get()
	defer conn.Close()

	input := args[0]

	var file *os.File
	var err error
	if input == "-" {
		// Discard stdin if it's connected to a terminal. We want it to be
		// connected to a pipe or a file.
		info, err := os.Stdin.Stat()
		if err != nil {
			logger.Fatalln("Error encountered while reading stdin", err)
		}
		if (info.Mode() & os.ModeCharDevice) == os.ModeCharDevice {
			logger.Fatalln("stdin must be a pipe or a file")
		}
		file = os.Stdin
	} else {
		file, err = os.Open(input)
		if err != nil {
			logger.Fatalln("Error encountered while reading file", err)
		}
		defer file.Close()
	}

	i := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		e, err := entry.NewNfdumpEntry(scanner.Text())
		if err != nil {
			continue
		}
		j, err := e.MarshalJSON()
		if err != nil {
			continue
		}
		if err := conn.Send("RPUSH", redisListKey, string(j)); err != nil {
			log.Println("Send() failed", err)
			continue
		}

		i++
		if i < *batchSize {
			if err := conn.Flush(); err != nil {
				log.Println("Flush() failed", err)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		logger.Println("Error encountered while reading file", err)
	}
}