package gobatsd

import (
	"fmt"
	"testing"
	"time"
)

func TestTimerUpdate(t *testing.T) {
	Config.Retentions = []Retention{{10, 10, 100, 0}}
	Config.RedisHost = "127.0.0.1"
	Config.RedisPort = 6379
	SetupDispatcher()

	timer := NewTimer("test").(*Timer)
	timer.Start()
	timer.Update(8.6754)
	time.Sleep(1 * time.Millisecond)
	if timer.Values[0][0] != 8.6754 || len(timer.Values[0]) != 1 {
		t.Errorf("Expected timer to update")
	}
	timer.save(Config.Retentions[0], time.Now())
}

func BenchmarkTimerUpdate(b *testing.B) {
	Config.Retentions = []Retention{{10, 10, 100, 0}}
	Config.RedisHost = "127.0.0.1"
	Config.RedisPort = 6379
	SetupDispatcher()

	timer := NewTimer("test").(*Timer)
	timer.Start()

	for j := 0; j < b.N; j++ {
		timer.Update(8.6754)
	}
}

func TestTimerSave(t *testing.T) {
	Config.Retentions = []Retention{{10, 10, 100, 0}, {20, 10, 200, 1}}
	Config.RedisHost = "127.0.0.1"
	Config.RedisPort = 6379
	SetupDispatcher()

	timer := NewTimer("testttttt").(*Timer)
	timer.Values[0] = []float64{1, 2, 3, 4, 5}
	now := time.Now()
	timer.save(Config.Retentions[0], now)
	obs := <-dispatcher.redisChannel
	expected := fmt.Sprintf("%v<X>%v", now.Unix()-now.Unix()%10, "5/1/5/3/3/1.5811388300841898/5/5/5")
	if obs.Content != expected {
		t.Errorf("Timer send to redis was not properly structured, got '%v' expected '%v'", obs.Content, expected)
	}
	timer.Values[1] = []float64{1, 2, 3, 4, 5}
	timer.save(Config.Retentions[1], now)
	obs = <-dispatcher.diskChannel
	expected = fmt.Sprintf("%v %v\n", now.Unix()-now.Unix()%20, "5/1/5/3/3/1.5811388300841898/5/5/5")
	if obs.Content != expected {
		t.Errorf("Timer send to disk was not properly structured, got '%v' expected '%v'", obs.Content, expected)
	}

}
