package gobatsd

import (
	"time"
)

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
