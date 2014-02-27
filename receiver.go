package main

import (
	"github.com/noahhl/clamp"
	"github.com/noahhl/go-batsd/gobatsd"

	"bufio"
	"fmt"
	"github.com/noahhl/Go-Redis"
	"github.com/reusee/mmh3"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type Datapoint struct {
	Timestamp time.Time
	Name      string
	Value     float64
	Datatype  string
}

type AggregateObservation struct {
	Name      string
	Content   string
	Timestamp int64
	RawName   string
}

var gaugeChannel chan Datapoint
var counterChannel chan Datapoint
var timerChannel chan Datapoint
var diskAppendChannel chan AggregateObservation
var redisAppendChannel chan AggregateObservation
var timerHeartbeat chan int
var counterHeartbeat chan int

const channelBufferSize = 10000
const heartbeatInterval = 1
const numIncomingMessageProcessors = 100

func main() {
	gobatsd.LoadConfig()
	gaugeChannel = make(chan Datapoint, channelBufferSize)
	counterChannel = make(chan Datapoint, channelBufferSize)
	timerChannel = make(chan Datapoint, channelBufferSize)
	counterHeartbeat = make(chan int)
	timerHeartbeat = make(chan int)

	fmt.Printf("Starting on port %v\n", gobatsd.Config.Port)
	runtime.GOMAXPROCS(runtime.NumCPU())

	datapointChannel := saveNewDatapoints()
	diskAppendChannel = appendToFile(datapointChannel)
	redisAppendChannel = addToRedisZset()

	processingChannel := clamp.StartDualServer(":8125")

	for i := 0; i < numIncomingMessageProcessors; i++ {
		launchMessageProcessor(processingChannel)
	}
	clamp.StartStatsServer(":8349")
	go runHeartbeat()

	go processGauges(gaugeChannel)
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
			counterHeartbeat <- 1
			timerHeartbeat <- 1
		}
	}
}

func launchMessageProcessor(ch chan string) {
	go func(channel chan string) {
		for {
			message := <-channel
			processIncomingMessage(message)
		}
	}(ch)
}

func processIncomingMessage(message string) {
	d := parseDatapoint(message)
	if d.Datatype == "g" {
		gaugeChannel <- d
	} else if d.Datatype == "c" {
		counterChannel <- d
	} else if d.Datatype == "ms" {
		timerChannel <- d
	}
}

func parseDatapoint(metric string) Datapoint {
	d := Datapoint{}
	components := strings.Split(metric, ":")
	if len(components) == 2 {
		latter_components := strings.Split(components[1], "|")
		if len(latter_components) >= 2 {
			value, _ := strconv.ParseFloat(latter_components[0], 64)
			if len(latter_components) == 3 && latter_components[1] == "c" {
				sample_rate, _ := strconv.ParseFloat(strings.Replace(latter_components[2], "@", "", -1), 64)
				value = value / sample_rate
			}
			d = Datapoint{time.Now(), components[0], value, latter_components[1]}
		}
	}
	return d
}

func saveNewDatapoints() chan string {
	c := make(chan string, channelBufferSize)

	go func(ch chan string) {
		spec := redis.DefaultSpec().Host(gobatsd.Config.RedisHost).Port(gobatsd.Config.RedisPort)
		redis, _ := redis.NewSynchClientWithSpec(spec)
		for {
			d := <-ch
			redis.Sadd("datapoints", []byte(d))
		}
	}(c)

	return c
}

func appendToFile(datapoints chan string) chan AggregateObservation {
	c := make(chan AggregateObservation, channelBufferSize)

	go func(ch chan AggregateObservation, datapoints chan string) {
		for {
			observation := <-ch
			filename := gobatsd.CalculateFilename(observation.Name, gobatsd.Config.Root)

			file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0600)
			newFile := false
			if err != nil {
				if e, ok := err.(*os.PathError); ok && e.Err == syscall.ENOENT {
					fmt.Printf("Creating %v\n", filename)
					//Make containing directories if they don't exist
					err = os.MkdirAll(filepath.Dir(filename), 0755)
					if err != nil {
						fmt.Printf("%v", err)
					}

					file, err = os.Create(filename)
					if err != nil {
						fmt.Printf("%v", err)
					}
					newFile = true
					datapoints <- observation.RawName
				} else {
					panic(err)
				}
			}
			if file != nil {
				writer := bufio.NewWriter(file)
				if newFile {
					writer.WriteString("v2 " + observation.Name + "\n")
				}
				writer.WriteString(observation.Content)
				writer.Flush()
				file.Close()
			}
		}
	}(c, datapoints)
	return c
}

func addToRedisZset() chan AggregateObservation {
	c := make(chan AggregateObservation, channelBufferSize)
	go func(ch chan AggregateObservation) {
		spec := redis.DefaultSpec().Host(gobatsd.Config.RedisHost).Port(gobatsd.Config.RedisPort)
		redis, _ := redis.NewSynchClientWithSpec(spec)
		for {
			observation := <-ch
			redis.Zadd(observation.Name, float64(observation.Timestamp), []byte(observation.Content))
		}
	}(c)

	return c

}

func processGauges(gauges chan Datapoint) {
	for {
		d := <-gauges
		//fmt.Printf("Processing gauge %v with value %v and timestamp %v \n", d.Name, d.Value, d.Timestamp)
		observation := AggregateObservation{"gauges:" + d.Name, fmt.Sprintf("%d %v\n", d.Timestamp.Unix(), d.Value), 0, "gauges:" + d.Name}
		diskAppendChannel <- observation
	}
}

type Counter struct {
	Key   string
	Value float64
}

func processCounters(ch chan Datapoint) {
	currentSlots := make([]int64, len(gobatsd.Config.Retentions))
	maxSlots := make([]int64, len(gobatsd.Config.Retentions))
	for i := range gobatsd.Config.Retentions {
		currentSlots[i] = 0
		maxSlots[i] = gobatsd.Config.Retentions[i].Interval / heartbeatInterval
	}

	counters := make([][]map[string]float64, len(gobatsd.Config.Retentions))
	for i := range counters {
		counters[i] = make([]map[string]float64, maxSlots[i])
		for j := range counters[i] {
			counters[i][j] = make(map[string]float64)
		}
	}

	for {
		select {
		case d := <-ch:
			//fmt.Printf("Processing counter %v with value %v and timestamp %v \n", d.Name, d.Value, d.Timestamp)
			for i := range gobatsd.Config.Retentions {
				hashSlot := int64(mmh3.Hash32([]byte(d.Name))) % maxSlots[i]
				counters[i][hashSlot][d.Name] += d.Value
			}

		case <-counterHeartbeat:
			for i := range currentSlots {
				timestamp := time.Now().Unix() - (time.Now().Unix() % gobatsd.Config.Retentions[i].Interval)
				for key, value := range counters[i][currentSlots[i]] {
					if value > 0 {
						if i == 0 { //Store to redis
							observation := AggregateObservation{"counters:" + key, fmt.Sprintf("%d<X>%v", timestamp, value), timestamp, "counters:" + key}
							redisAppendChannel <- observation
						} else {
							observation := AggregateObservation{"counters:" + key + ":" + strconv.FormatInt(gobatsd.Config.Retentions[i].Interval, 10), fmt.Sprintf("%d %v\n", timestamp, value), timestamp, "counters:" + key}
							diskAppendChannel <- observation
						}
						delete(counters[i][currentSlots[i]], key)
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

func processTimers(ch chan Datapoint) {

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
							observation := AggregateObservation{"timers:" + key, fmt.Sprintf("%d<X>%v", timestamp, aggregates), timestamp, "timers:" + key}
							redisAppendChannel <- observation
						} else { // Store to disk
							observation := AggregateObservation{"timers:" + key + ":" + strconv.FormatInt(gobatsd.Config.Retentions[i].Interval, 10) + ":2", fmt.Sprintf("%d %v\n", timestamp, aggregates), timestamp, "timers:" + key}
							diskAppendChannel <- observation
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
