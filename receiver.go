package main

import (
	"github.com/noahhl/clamp"
	"github.com/noahhl/go-batsd/gobatsd"

	"fmt"
	"runtime"
	"time"
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
	clamp.StartStatsServer(":8124")
	gobatsd.SetupDatastore()

	fmt.Printf("Starting on port %v\n", gobatsd.Config.Port)

	gaugeChannel = make(chan gobatsd.Datapoint, channelBufferSize)
	counterChannel = make(chan gobatsd.Datapoint, channelBufferSize)
	timerChannel = make(chan gobatsd.Datapoint, channelBufferSize)

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

	go func() {
		c := time.Tick(5 * time.Second)
		for {
			<-c
			clamp.StatsChannel <- clamp.Stat{"gaugeChannelSize", fmt.Sprintf("%v", len(gaugeChannel))}
			clamp.StatsChannel <- clamp.Stat{"counterChannelSize", fmt.Sprintf("%v", len(counterChannel))}
			clamp.StatsChannel <- clamp.Stat{"timerChannelSize", fmt.Sprintf("%v", len(timerChannel))}
		}
	}()

	processDatatype("gauges", gaugeChannel, gobatsd.NewGauge)
	processDatatype("timers", timerChannel, gobatsd.NewTimer)
	processDatatype("counters", counterChannel, gobatsd.NewCounter)

	c := make(chan int)
	for {
		<-c
	}

}

func processDatatype(datatypeName string, ch chan gobatsd.Datapoint, metricCreator func(string) gobatsd.Metric) {
	metrics := make(map[string]gobatsd.Metric)
	go func() {
		c := time.Tick(5 * time.Second)
		for {
			<-c
			clamp.StatsChannel <- clamp.Stat{datatypeName, fmt.Sprintf("%v", len(metrics))}
		}
	}()
	go func() {
		for {
			select {
			case d := <-ch:
				if m, ok := metrics[d.Name]; ok {
					m.Update(d.Value)
				} else {
					m := metricCreator(d.Name)
					m.Start()
					metrics[d.Name] = m
					m.Update(d.Value)
				}
			}
		}
	}()

}
