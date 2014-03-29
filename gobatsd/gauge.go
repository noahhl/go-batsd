package gobatsd

import (
	"fmt"
	"time"
)

type Gauge struct {
	Key string
}

const gaugeInternalBufferSize = 10

func NewGauge(name string) Metric {
	g := &Gauge{}
	g.Key = name
	return g
}

func (g *Gauge) Start() {
}

func (g *Gauge) Update(value float64) {
	observation := AggregateObservation{"gauges:" + g.Key, fmt.Sprintf("%d %v\n", time.Now().Unix(), value), 0, "gauges:" + g.Key}
	StoreOnDisk(observation)
}
