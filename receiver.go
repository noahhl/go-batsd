package main

import (
	"github.com/noahhl/clamp"
	"github.com/noahhl/go-batsd/gobatsd"

	"fmt"
	"runtime"
)

var counterChannel chan gobatsd.Datapoint
var gaugeChannel chan gobatsd.Datapoint
var timerChannel chan gobatsd.Datapoint

const channelBufferSize = 10000
const heartbeatInterval = 1
const numIncomingMessageProcessors = 100

func main() {
	gobatsd.LoadConfig()
	runtime.GOMAXPROCS(runtime.NumCPU())
	processingChannel := clamp.StartDualServer(":8125")
	clamp.StartStatsServer(":8349")
	gobatsd.SetupDispatcher()

	gaugeChannel = make(chan gobatsd.Datapoint, channelBufferSize)
	counterChannel = make(chan gobatsd.Datapoint, channelBufferSize)
	timerChannel = make(chan gobatsd.Datapoint, channelBufferSize)

	fmt.Printf("Starting on port %v\n", gobatsd.Config.Port)
	channels := map[string]chan gobatsd.Datapoint{"g": gaugeChannel, "c": counterChannel, "ms": timerChannel}

	for i := 0; i < numIncomingMessageProcessors; i++ {
		go func(processingChannel chan string) {
			for {
				message := <-processingChannel
				d := gobatsd.ParseDatapointFromString(message)
				if ch, ok := channels[d.Datatype]; ok {
					ch <- d
				}
			}
		}(processingChannel)
	}

	go processGauges(gaugeChannel)
	go processCounters(counterChannel)
	go processTimers(timerChannel)

	c := make(chan int)
	for {
		<-c
	}

}

func processGauges(ch chan gobatsd.Datapoint) {
	gauges := make(map[string]*gobatsd.Gauge)

	for {
		select {
		case d := <-ch:
			if gauge, ok := gauges[d.Name]; ok {
				gauge.Update(d.Value)
			} else {
				gauge := gobatsd.NewGauge(d.Name)
				gauge.Start()
				gauges[d.Name] = gauge
				gauge.Update(d.Value)
			}
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

	timers := make(map[string]*gobatsd.Timer)

	for {
		select {
		case d := <-ch:
			if timer, ok := timers[d.Name]; ok {
				timer.Update(d.Value)
			} else {
				timer := gobatsd.NewTimer(d.Name)
				timer.Start()
				timers[d.Name] = timer
				timer.Update(d.Value)
			}
		}
	}
}
