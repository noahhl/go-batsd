package gobatsd

import (
	"fmt"
)

type GaugeHandler struct {
	ch chan Datapoint
}

func NewGaugeHandler() *GaugeHandler {
	h := GaugeHandler{}
	h.ch = make(chan Datapoint, channelBufferSize)

	return &h
}

func (g *GaugeHandler) ProcessNewDatapoint(d Datapoint) {
	//fmt.Printf("Processing gauge %v with value %v and timestamp %v \n", d.Name, d.Value, d.Timestamp)
	observation := AggregateObservation{"gauges:" + d.Name, fmt.Sprintf("%d %v\n", d.Timestamp.Unix(), d.Value), 0, "gauges:" + d.Name}
	StoreOnDisk(observation)
}
