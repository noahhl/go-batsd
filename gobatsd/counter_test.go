package gobatsd

import (
	"fmt"
	"testing"
	"time"
)

func TestCounterIncrement(t *testing.T) {
	Config.Retentions = []Retention{{10, 10, 100, 0}}
	Config.RedisHost = "127.0.0.1"
	Config.RedisPort = 6379
	SetupDispatcher()

	c := NewCounter("test")
	c.Start()
	c.Update(1.3)
	time.Sleep(1 * time.Millisecond)
	if c.Values[0] != 1.3 {
		t.Errorf("Expected counter to increment")
	}
	c.save(Config.Retentions[0], time.Now())
}

func BenchmarkCounterIncrement(b *testing.B) {
	Config.Retentions = []Retention{{10, 10, 100, 0}}
	Config.RedisHost = "127.0.0.1"
	Config.RedisPort = 6379
	SetupDispatcher()

	c := NewCounter("test")
	c.Start()
	for j := 0; j < b.N; j++ {
		c.Update(1)
	}
}

func TestCounterSave(t *testing.T) {
	Config.Retentions = []Retention{{10, 10, 100, 0}, {20, 10, 200, 1}}
	Config.RedisHost = "127.0.0.1"
	Config.RedisPort = 6379
	SetupDispatcher()
	c := NewCounter("test")
	c.Values[0] = 123
	now := time.Now()
	c.save(Config.Retentions[0], now)
	obs := <-dispatcher.redisChannel
	expected := fmt.Sprintf("%v<X>%v", now.Unix()-now.Unix()%10, 123)
	if obs.Content != expected {
		t.Errorf("Counter send to redis was not properly structured, got '%v' expected '%v'", obs.Content, expected)
	}
	c.Values[1] = 123
	c.save(Config.Retentions[1], now)
	obs = <-dispatcher.diskChannel
	expected = fmt.Sprintf("%v %v\n", now.Unix()-now.Unix()%20, 123)
	if obs.Content != expected {
		t.Errorf("Counter send to disk was not properly structured, got '%v' expected '%v'", obs.Content, expected)
	}

}
