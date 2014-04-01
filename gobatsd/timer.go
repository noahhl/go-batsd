package gobatsd

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"
)

type Timer struct {
	Key      string
	Values   [][]float64
	channels []chan float64
	Paths    []string
}

const timerInternalBufferSize = 10
const timerVersion = "2"

func NewTimer(name string) Metric {
	t := &Timer{}
	t.Key = name
	t.Values = make([][]float64, len(Config.Retentions))
	for i := range t.Values {
		t.Values[i] = make([]float64, 0)
	}
	t.channels = make([]chan float64, len(Config.Retentions))
	for i := range t.channels {
		t.channels[i] = make(chan float64, timerInternalBufferSize)
	}
	t.Paths = make([]string, len(Config.Retentions))
	for i := range t.Paths {
		t.Paths[i] = CalculateFilename(fmt.Sprintf("timers:%v:%v:%v", t.Key, Config.Retentions[i].Interval, timerVersion), Config.Root)
	}
	datastore.RecordMetric(fmt.Sprintf("timers:%v", t.Key))
	t.Start()
	return t
}

func (t *Timer) Start() {
	for i := range Config.Retentions {
		go func(retention Retention) {
			ticker := NewTickerWithOffset(time.Duration(retention.Interval)*time.Second,
				time.Duration(rand.Intn(int(retention.Interval)))*time.Second)
			for {
				select {
				case now := <-ticker:
					//fmt.Printf("%v: Time to save %v at retention %v\n", now, c.Key, retention)
					t.save(retention, now)
				case val := <-t.channels[retention.Index]:
					t.Values[retention.Index] = append(t.Values[retention.Index], val)
				}
			}
		}(Config.Retentions[i])

	}
}

func (t *Timer) Update(value float64) {
	for i := range t.channels {
		t.channels[i] <- value
	}
}

func (t *Timer) save(retention Retention, now time.Time) {
	values := t.Values[retention.Index]
	t.Values[retention.Index] = make([]float64, 0)
	if len(values) == 0 {
		return
	}

	go func() {
		timestamp := now.Unix() - now.Unix()%retention.Interval
		count := len(values)
		min := Min(values)
		max := Max(values)
		median := Median(values)
		mean := Mean(values)
		stddev := Stddev(values)
		percentile_90 := Percentile(values, 0.9)
		percentile_95 := Percentile(values, 0.95)
		percentile_99 := Percentile(values, 0.99)
		aggregates := fmt.Sprintf("%v/%v/%v/%v/%v/%v/%v/%v/%v", count, min, max, median, mean, stddev, percentile_90, percentile_95, percentile_99)

		if retention.Index == 0 {
			observation := AggregateObservation{"timers:" + t.Key, fmt.Sprintf("%d<X>%v", timestamp, aggregates), timestamp, "timers:" + t.Key, ""}
			StoreInRedis(observation)
		} else {
			observation := AggregateObservation{"timers:" + t.Key + ":" + strconv.FormatInt(retention.Interval, 10) + ":" + timerVersion, fmt.Sprintf("%d %v\n", timestamp, aggregates), timestamp, "timers:" + t.Key, t.Paths[retention.Index]}
			StoreOnDisk(observation)
		}
	}()
}
