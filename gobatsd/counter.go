package gobatsd

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"
)

type Counter struct {
	Key      string
	Values   []float64
	channels []chan float64
	Paths    []string
}

const counterInternalBufferSize = 10

func NewCounter(name string) Metric {
	c := &Counter{}
	c.Key = name
	c.Values = make([]float64, len(Config.Retentions))
	c.channels = make([]chan float64, len(Config.Retentions))
	for i := range c.channels {
		c.channels[i] = make(chan float64, counterInternalBufferSize)
	}
	c.Paths = make([]string, len(Config.Retentions))
	for i := range c.Paths {
		c.Paths[i] = CalculateFilename(fmt.Sprintf("counters:%v:%v", c.Key, Config.Retentions[i].Interval), Config.Root)
	}
	datastore.RecordMetric(fmt.Sprintf("counters:%v", c.Key))
	c.Start()
	return c
}

func (c *Counter) Start() {
	for i := range Config.Retentions {
		go func(retention Retention) {
			ticker := NewTickerWithOffset(time.Duration(retention.Interval)*time.Second,
				time.Duration(rand.Intn(int(retention.Interval)))*time.Second)
			for {
				select {
				case now := <-ticker:
					//fmt.Printf("%v: Time to save %v at retention %v\n", now, c.Key, retention)
					c.save(retention, now)
				case val := <-c.channels[retention.Index]:
					c.Values[retention.Index] += val
				}
			}
		}(Config.Retentions[i])

	}
}

func (c *Counter) Update(value float64) {
	for i := range c.channels {
		c.channels[i] <- value
	}
}

func (c *Counter) save(retention Retention, now time.Time) {
	aggregateValue := c.Values[retention.Index]
	c.Values[retention.Index] = 0
	timestamp := now.Unix() - now.Unix()%retention.Interval
	//fmt.Printf("%v: Ready to store %v, value now %v, retention #%v\n", timestamp, aggregateValue, c.Values[retention.Index], retention.Index)
	if aggregateValue == 0 {
		return
	}

	if retention.Index == 0 {
		observation := AggregateObservation{Name: "counters:" + c.Key, Content: fmt.Sprintf("%d<X>%v", timestamp, aggregateValue), Timestamp: timestamp, RawName: "counters:" + c.Key, Path: ""}
		StoreInRedis(observation)
	} else {
		observation := AggregateObservation{Name: "counters:" + c.Key + ":" + strconv.FormatInt(retention.Interval, 10), Content: fmt.Sprintf("%d %v\n", timestamp, aggregateValue),
			Timestamp: timestamp, RawName: "counters:" + c.Key, Path: c.Paths[retention.Index], SummaryValues: map[string]float64{"value": aggregateValue}, Interval: retention.Interval}
		StoreOnDisk(observation)
	}
}
