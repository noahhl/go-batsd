package gobatsd

import (
	"fmt"
	"time"
)

type Gauge struct {
	Key  string
	Path string
}

const gaugeInternalBufferSize = 10

func NewGauge(name string) Metric {
	g := &Gauge{}
	g.Key = name
	g.Path = CalculateFilename("gauges:"+g.Key, Config.Root)
	datastore.RecordMetric(fmt.Sprintf("gauges:%v", g.Key))
	return g
}

func (g *Gauge) Start() {
}

func (g *Gauge) Update(value float64) {
	observation := AggregateObservation{"gauges:" + g.Key, fmt.Sprintf("%d %v\n", time.Now().Unix(), value), 0, "gauges:" + g.Key, g.Path}
	StoreOnDisk(observation)
}
