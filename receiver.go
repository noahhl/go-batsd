package main

import (
	"github.com/noahhl/clamp"
	"github.com/noahhl/go-batsd/gobatsd"

	"fmt"
	"github.com/reusee/mmh3"
	"runtime"
	"strconv"
	"time"
)

var counterChannel chan gobatsd.Datapoint
var timerChannel chan gobatsd.Datapoint
var timerHeartbeat chan int
var counterHeartbeat chan int

const channelBufferSize = 10000
const heartbeatInterval = 1
const numIncomingMessageProcessors = 100

func main() {
	gobatsd.LoadConfig()
	runtime.GOMAXPROCS(runtime.NumCPU())
	processingChannel := clamp.StartDualServer(":8125")
	clamp.StartStatsServer(":8349")
	gobatsd.SetupDispatcher()

	gaugeHandler := gobatsd.NewGaugeHandler()
	counterChannel = make(chan gobatsd.Datapoint, channelBufferSize)
	timerChannel = make(chan gobatsd.Datapoint, channelBufferSize)
	counterHeartbeat = make(chan int)
	timerHeartbeat = make(chan int)

	fmt.Printf("Starting on port %v\n", gobatsd.Config.Port)

	for i := 0; i < numIncomingMessageProcessors; i++ {
		go func(processingChannel chan string) {
			for {
				message := <-processingChannel
				d := gobatsd.ParseDatapointFromString(message)
				if d.Datatype == "g" {
					gaugeHandler.ProcessNewDatapoint(d)
				} else if d.Datatype == "c" {
					counterChannel <- d
				} else if d.Datatype == "ms" {
					timerChannel <- d
				}
			}
		}(processingChannel)
	}

	go runHeartbeat()
	go processCounters(counterChannel)
	go processTimers(timerChannel)

	c := make(chan int)
	for {
		<-c
	}

}

func runHeartbeat() {
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ticker.C:
			timerHeartbeat <- 1
		}
	}
}

func processCounters(ch chan gobatsd.Datapoint) {
	counters := make(map[string]*gobatsd.Counter)

	for {
		select {
		case d := <-ch:
			if counter, ok := counters[d.Name]; ok {
				counter.Increment(d.Value)
			} else {
				counter := gobatsd.NewCounter(d.Name)
				counter.Start()
				counters[d.Name] = counter
				counter.Increment(d.Value)
			}
		}
	}
}

func processTimers(ch chan gobatsd.Datapoint) {

	currentSlots := make([]int64, len(gobatsd.Config.Retentions))
	maxSlots := make([]int64, len(gobatsd.Config.Retentions))
	for i := range gobatsd.Config.Retentions {
		currentSlots[i] = 0
		maxSlots[i] = gobatsd.Config.Retentions[i].Interval / heartbeatInterval
	}

	timers := make([][]map[string][]float64, len(gobatsd.Config.Retentions))
	for i := range timers {
		timers[i] = make([]map[string][]float64, maxSlots[i])
		for j := range timers[i] {
			timers[i][j] = make(map[string][]float64)
		}
	}

	for {
		select {
		case d := <-ch:
			//fmt.Printf("Processing timer %v with value %v and timestamp %v \n", d.Name, d.Value, d.Timestamp)
			for i := range gobatsd.Config.Retentions {
				hashSlot := int64(mmh3.Hash32([]byte(d.Name))) % maxSlots[i]
				timers[i][hashSlot][d.Name] = append(timers[i][hashSlot][d.Name], d.Value)
			}
		case <-timerHeartbeat:
			for i := range currentSlots {
				//fmt.Printf("%v %v %v\n", i, currentSlots[i], timers[i][currentSlots[i]])

				timestamp := time.Now().Unix() - (time.Now().Unix() % gobatsd.Config.Retentions[i].Interval)

				for key, value := range timers[i][currentSlots[i]] {
					if len(value) > 0 {
						count := len(value)
						min := gobatsd.Min(value)
						max := gobatsd.Max(value)
						median := gobatsd.Median(value)
						mean := gobatsd.Mean(value)
						stddev := gobatsd.Stddev(value)
						percentile_90 := gobatsd.Percentile(value, 0.9)
						percentile_95 := gobatsd.Percentile(value, 0.95)
						percentile_99 := gobatsd.Percentile(value, 0.99)

						aggregates := fmt.Sprintf("%v/%v/%v/%v/%v/%v/%v/%v/%v", count, min, max, median, mean, stddev, percentile_90, percentile_95, percentile_99)
						if i == 0 { //Store to redis
							observation := gobatsd.AggregateObservation{"timers:" + key, fmt.Sprintf("%d<X>%v", timestamp, aggregates), timestamp, "timers:" + key}
							gobatsd.StoreInRedis(observation)
						} else { // Store to disk
							observation := gobatsd.AggregateObservation{"timers:" + key + ":" + strconv.FormatInt(gobatsd.Config.Retentions[i].Interval, 10) + ":2", fmt.Sprintf("%d %v\n", timestamp, aggregates), timestamp, "timers:" + key}
							gobatsd.StoreOnDisk(observation)
						}

						delete(timers[i][currentSlots[i]], key)
					}
				}

				currentSlots[i] += 1
				if currentSlots[i] == maxSlots[i] {
					currentSlots[i] = 0
				}
			}

		}
	}
}
