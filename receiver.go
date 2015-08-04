package main

import (
	"github.com/noahhl/clamp"
	"github.com/noahhl/go-batsd/gobatsd"

	"fmt"
	"runtime"
	"time"

	"os"
	"os/signal"
	"runtime/pprof"
)

var counterChannel chan gobatsd.Datapoint
var gaugeChannel chan gobatsd.Datapoint
var timerChannel chan gobatsd.Datapoint

const channelBufferSize = 2000000
const numIncomingMessageProcessors = 20

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	gobatsd.LoadConfig()
	if gobatsd.ProfileCPU {
		cpuprof, err := os.Create(fmt.Sprintf("cpuprof-%v", time.Now().Unix()))
		if err != nil {
			panic(err)
		}
		defer cpuprof.Close()
		pprof.StartCPUProfile(cpuprof)
		defer pprof.StopCPUProfile()
	}

	processingChannel := clamp.StartExplodingDualServer(fmt.Sprintf("%s:%s", gobatsd.Config.BindHost, gobatsd.Config.Port))
	clamp.StartStatsServer(fmt.Sprintf("%s:%s", gobatsd.Config.BindHost, 8124))

	gobatsd.SetupDatastore()

	fmt.Printf("Starting on host:port %s:%s\n", gobatsd.Config.BindHost, gobatsd.Config.Port)
	gobatsd.InitializeInternalMetrics()

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
		c := time.Tick(1 * time.Second)
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

	terminate := make(chan os.Signal)
	signal.Notify(terminate, os.Interrupt)
	<-terminate

	fmt.Printf("Server stopped")

}

func processDatatype(datatypeName string, ch chan gobatsd.Datapoint, metricCreator func(string) gobatsd.Metric) {
	metrics := make(map[string]gobatsd.Metric)
	go func() {
		c := time.Tick(1 * time.Second)
		for {
			<-c
			clamp.StatsChannel <- clamp.Stat{datatypeName, fmt.Sprintf("%v", len(metrics))}
			gobatsd.InternalMetrics[datatypeName+"Known"].Update(float64(len(metrics)))
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
					metrics[d.Name] = m
					m.Update(d.Value)
				}
				gobatsd.InternalMetrics[datatypeName+"Processed"].Update(1)
			}
		}
	}()

}
