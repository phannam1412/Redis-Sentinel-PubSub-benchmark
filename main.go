package main

import (
    "github.com/go-redis/redis"
    "fmt"
    "time"
    "gopkg.in/cheggaaa/pb.v1"
    "math"
    "strconv"
    "os"
    "sync"
)

type RedisService struct {
    conn *redis.Client
}

func PanicOnError(err error) {
    if err != nil {
        panic(err)
    }
}

func NewRedisService() (*RedisService, error) {
    client := redis.NewClient(&redis.Options{
        Addr: "redis:6379",
        Password: "",
        DB: 0,
    })
    _, err := client.Ping().Result()

    if err != nil {
        return nil, err
    }
    return &RedisService{
        conn: client,
    }, nil
}

func NewFailoverRedisService(SentinelHost string, SentinelMaster string) (*RedisService, error) {

    client := redis.NewFailoverClient(&redis.FailoverOptions{
       MasterName: SentinelMaster,
       SentinelAddrs: []string{SentinelHost},
    })

    _, err := client.Ping().Result()

    if err != nil {
        return nil, err
    }

    return &RedisService{
        conn: client,
    }, nil
}

func (s *RedisService) Publish(key string, value string) error {
    return s.conn.Publish(key, value).Err()
}

func (s *RedisService) Close() error {
    return s.conn.Close()
}

func (s *RedisService) Subscribe(key string) (<-chan *redis.Message, error) {

    pubsub := s.conn.Subscribe(key)

    // Wait for confirmation that subscription is created before publishing anything.
    _, err := pubsub.Receive()
    if err != nil {
        return nil, err
    }

    // Go channel which receives messages.
    return pubsub.Channel(), nil
}


func benchmark(numberOfChannels int, numberOfGoRoutinesForPublishingMessages int, numberOfClients int, numberOfRequests int, SentinelHost string, SentinelMaster string) {

    var err error
    val := "hello world"
    redisChannel := "nam_test"
    haveReceivedEnoughMessages := make(chan bool)
    maximumMessagesReceived := int64(numberOfClients * numberOfRequests / numberOfChannels)

    redisService, err := NewFailoverRedisService(SentinelHost, SentinelMaster)
    PanicOnError(err)

    var waitForAllPublishers sync.WaitGroup
    waitForAllPublishers.Add(numberOfGoRoutinesForPublishingMessages)
    numberOfRequestsForEachGoRoutine :=  numberOfRequests / numberOfGoRoutinesForPublishingMessages

    // Progress bar showing publishing progress.
    publisherBar := pb.New(numberOfRequests).
        SetMaxWidth(100).
        Prefix("Publish  ")
    publisherBar.ShowTimeLeft = false
    publisherBar.ShowElapsedTime = true
    publisherBar.Start()

    // Progress bar showing subscribing progress.
    subscriberBar := pb.New(numberOfRequests * numberOfClients / numberOfChannels).
        SetMaxWidth(100).
        Prefix("Subscribe")
    subscriberBar.ShowTimeLeft = false
    subscriberBar.ShowElapsedTime = true
    subscriberBar.Start()

    // Display the progress bars all together.
    barPool, err := pb.StartPool(publisherBar, subscriberBar)
    PanicOnError(err)

    // Receive message and print to stdout.
    start := time.Now()
    suffix := string(start.Unix())

    // Establish many clients connecting to Redis.
    for a := 0; a < numberOfClients; a++ {
        c, err := redisService.Subscribe(redisChannel + "_" + strconv.Itoa(a % numberOfChannels) + "_" + suffix)
        PanicOnError(err)
        go func(channel <-chan *redis.Message) {
            // Client to receive messages.
            for {
                select {
                case <-channel:
                    subscriberBar.Increment()

                    // Received enough messages for stress test ?
                    if subscriberBar.Get() >= maximumMessagesReceived {
                        haveReceivedEnoughMessages <- true
                        return
                    }
                }
            }
        }(c)
    }

    // Spawn many goroutines for publishing messages.
    for i:= 0; i < numberOfGoRoutinesForPublishingMessages;i++ {
        go func() {
            pub, err := NewFailoverRedisService(SentinelHost, SentinelMaster)
            PanicOnError(err)

            // Publish messages.
            for a := 0; a < numberOfRequestsForEachGoRoutine; a++ {
                err := pub.Publish(redisChannel + "_" + strconv.Itoa(a % numberOfChannels)  + "_" + suffix, val)
                PanicOnError(err)
                publisherBar.Increment()
            }

            PanicOnError(pub.Close())
            waitForAllPublishers.Done()
        }()
    }

    <-haveReceivedEnoughMessages

    barPool.Stop()
    fmt.Printf("\n%v channels, %v goroutines, %v clients, %v requests, time: %v, rps: %v\n\n", numberOfChannels, numberOfGoRoutinesForPublishingMessages, numberOfClients, numberOfRequests, time.Since(start), math.Round(float64(numberOfRequests) / time.Since(start).Seconds()))

    waitForAllPublishers.Wait()

    PanicOnError(redisService.Close())
}

func main() {

    // Usage:
    // ./test-redis-pub-sub [number of goroutines for publishing messages] [number of clients] [number of messages to be published] [sentinel host and port] [redis master]
    if len(os.Args) == 6 {
        goroutines, err := strconv.Atoi(os.Args[1])
        PanicOnError(err)
        clients, err := strconv.Atoi(os.Args[2])
        PanicOnError(err)
        messages, err := strconv.Atoi(os.Args[3])
        PanicOnError(err)
        benchmark(1, goroutines, clients, messages, os.Args[4], os.Args[5])
        return
    }

    panic("invalid number of arguments")

}
