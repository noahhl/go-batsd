package gobatsd

import (
	"fmt"
	"math/rand"
	"sort"
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
		sort.Float64s(values)
		count := len(values)
		min := SortedMin(values)
		max := SortedMax(values)
		median := SortedMedian(values)
		mean := Mean(values)
		stddev := Stddev(values)
		percentile_90 := SortedPercentile(values, 0.9)
		percentile_95 := SortedPercentile(values, 0.95)
		percentile_99 := SortedPercentile(values, 0.99)
		aggregates := fmt.Sprintf("%v/%v/%v/%v/%v/%v/%v/%v/%v", count, min, max, median, mean, stddev, percentile_90, percentile_95, percentile_99)

		if retention.Index == 0 {
			observation := AggregateObservation{Name: "timers:" + t.Key, Content: fmt.Sprintf("%d<X>%v", timestamp, aggregates), Timestamp: timestamp, RawName: "timers:" + t.Key, Path: ""}
			StoreInRedis(observation)
		} else {
			observation := AggregateObservation{Name: "timers:" + t.Key + ":" + strconv.FormatInt(retention.Interval, 10) + ":" + timerVersion,
				Content: fmt.Sprintf("%d %v\n", timestamp, aggregates), Timestamp: timestamp, RawName: "timers:" + t.Key, Path: t.Paths[retention.Index],
				SummaryValues: map[string]float64{"count": float64(count), "min": min, "max": max, "median": median, "mean": mean, "stddev": stddev, "percentile_90": percentile_90, "percentile_95": percentile_95, "percentile_99": percentile_99}, Interval: retention.Interval}
			StoreOnDisk(observation)
		}
	}()
}
