# Benchmark Redis Sentinel pub-sub

### Setup
$ go get github.com/go-redis/redis
$ go get gopkg.in/cheggaaa/pb.v1

### Usage

go run main.go [number of goroutines for publishing messages] [number of clients] [number of messages to be published] [sentinel host and port] [redis master]

e.g. $ go run 100 50 1000000 sentinel:26379 mymaster
