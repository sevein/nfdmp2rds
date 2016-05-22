## nfdmp2rds

### Installation

Binaries are not available. You need to [install Go](https://golang.org/doc/install), which is always a good idea :)

    $ go get -u github.com/sevein/nfdmp2rds

### Usage examples

Help:

    $ nfdmp2rds -h

You can pipe the input:

    $ cat test.txt | nfdmp2rds -

Or pass a filename:

    $ nfdmp2rds test.txt

It's possible to adjust the size of the `RPUSH` batches (pipelining):

    $ nfdmp2rds -bsize 1000 test.txt

Full example:

    $ nfdmp2rds -bsize 1000 -redisListKey foobar -redisServer 127.0.0.1:6379 -redisPassword foobar test.txt
