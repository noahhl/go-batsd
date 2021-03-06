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
	return g
}

func (g *Gauge) Start() {
}

func (g *Gauge) Update(value float64) {
	observation := AggregateObservation{Name: "gauges:" + g.Key, Content: fmt.Sprintf("%d %v\n", time.Now().Unix(), value), Timestamp: time.Now().Unix(), RawName: "gauges:" + g.Key,
		Path: g.Path, SummaryValues: map[string]float64{"value": value}, Interval: 0}
	StoreOnDisk(observation)
	datastore.RecordCurrent("gauges:"+g.Key, value)
}
