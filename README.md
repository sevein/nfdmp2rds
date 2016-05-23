## nfdmp2rds

### Installation

Binaries are not available. [Install Go](https://golang.org/doc/install) set your `GOPATH` and run the following:

    $ go get -u github.com/sevein/nfdmp2rds

Run it again to build the latest sources available!

### Usage examples

You can pipe the input:

    $ cat test.txt | nfdmp2rds netflow:test001 -

Or pass a filename:

    $ nfdmp2rds netflow:test001 test.txt

Detailed example:

    $ nfdmp2rds -bsize 1000 -redisServer 127.0.0.1:6379 netflow:test001 test.txt

Help:

```
$ nfdmp2rds -h
Usage: nfdmp2rds [options] redisListKey file
(redisListKey and file mandatory)

Flags (options):
  -bsize int
        Batch size (default 5)
  -cpuprofile string
        Write CPU profile to file
  -flush
        Delete key
  -h	Print command usage help
  -hostname string
        Given hostname (default "localhost")
  -redisPassword string
        Redis password
  -redisServer string
        Redis server (default ":6379")
  -v	Verbose mode
  -workers int
        Number of workers (default 2)
```