package gobatsd

import (
	"time"
)

var InternalMetrics map[string]Metric

func InitializeInternalMetrics() {
	InternalMetrics = map[string]Metric{
		"countersProcessed": NewCounter("statsd.countersProcessed"),
		"gaugesProcessed":   NewCounter("statsd.gaugesProcessed"),
		"timersProcessed":   NewCounter("statsd.timersProcessed"),
		"countersKnown":     NewTimer("statsd.countersKnown"),
		"gaugesKnown":       NewTimer("statsd.gaugesKnown"),
		"timersKnown":       NewTimer("statsd.timersKnown"),
	}

}

func NewTickerWithOffset(frequency time.Duration, offset time.Duration) chan time.Time {
	ch := make(chan time.Time)
	go func() {
		time.Sleep(offset)
		ch <- time.Now()
		c := time.Tick(frequency)
		for now := range c {
			ch <- now
		}
	}()
	return ch
}
