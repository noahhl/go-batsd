package gobatsd

import (
	"github.com/noahhl/Go-Redis"
	"github.com/noahhl/clamp"
	"math/rand"
	"testing"
	"time"
)

func TestFilenameCalculation(t *testing.T) {
	filename := CalculateFilename("test_metric", "/u/batsd")
	expected := "/u/batsd/34/1e/341e012c2e30d7853542921c1d76c8da"
	if filename != expected {
		t.Errorf("Expected filename to be %v, was %v\n", expected, filename)
	}
}

func BenchmarkFilenameCalculation(b *testing.B) {
	samples := []string{"timers:sysstat.statsd-101.bread/s.8822.00", "timers:sysstat.statsd-101.rtps.3703.00", "timers:sysstat.statsd-101.rtps.5161.00", "timers:sysstat.statsd-101.wtps.3033.00", "gauges:Syslog-NG.syslog-102.destination.d_app_writeboard_staging.empty.a.processed", "timers:sysstat.statsd-101.bread/s.3965.00", "timers:sysstat.statsd-101.bwrtn/s.3037.00", "timers:sysstat.statsd-101.rtps.8183.00", "timers:sysstat.statsd-101.rtps.6725.00", "timers:sysstat.statsd-101.wtps.6055.00", "timers:sysstat.statsd-101.bread/s.6987.00", "timers:sysstat.statsd-101.bwrtn/s.7540.00", "timers:sysstat.statsd-101.rtps.1868.00", "timers:sysstat.statsd-101.wtps.1198.00", "timers:sysstat.statsd-101.bwrtn/s.2683.00", "timers:sysstat.statsd-101.rtps.317.00", "timers:sysstat.bcx-101.bread/s.443.00", "timers:sysstat.statsd-101.wtps.7619.00", "timers:sysstat.statsd-101.wtps.9077.00", "gauges:Syslog-NG.bcx-109.src_internal.s_local_2.empty.a.stamp", "timers:sysstat.statsd-101.pgpgout/s.176.00", "timers:sysstat.statsd-101.bread/s.1063.00", "timers:sysstat.statsd-101.cswch/s.121.00", "timers:sysstat.statsd-102.bwrtn/s.428.00", "timers:sysstat.bcx-101.wtps.6269.00", "timers:sysstat.statsd-101.bread/s.2627.00", "timers:sysstat.statsd-101.bread/s.4085.00", "timers:sysstat.statsd-101.wtps.1341.00", "timers:sysstat.statsd-101.bread/s.5649.00", "timers:sysstat.bcx-101.rtps.14.00", "timers:sysstat.statsd-101.bwrtn/s.6202.00", "timers:sysstat.statsd-101.bread/s.317.00", "timers:sysstat.statsd-101.bwrtn/s.1345.00", "timers:sysstat.statsd-101.wtps.4363.00", "timers:sysstat.statsd-101.rtps.8409.00", "timers:sysstat.statsd-101.wtps.2905.00", "timers:sysstat.statsd-101.bwrtn/s.9224.00", "timers:sysstat.statsd-101.bwrtn/s.4367.00", "timers:sysstat.statsd-101.bwrtn/s.2909.00", "timers:sysstat.statsd-101.wtps.5927.00", "timers:sysstat.statsd-101.wtps.7385.00", "counters:memcached.shr-memory-102.11212.slab20.cas_hits", "timers:sysstat.statsd-101.bwrtn/s.8870.00", "timers:sysstat.statsd-101.bwrtn/s.7389.00", "timers:sysstat.statsd-101.wtps.8949.00", "timers:sysstat.statsd-102.bwrtn/s.194.00", "timers:sysstat.statsd-101.bwrtn/s.925.00", "timers:sysstat.statsd-101.rtps.2131.00", "timers:sysstat.statsd-101.bread/s.2393.00", "timers:sysstat.bcx-101.meff.3292.00"}
	for j := 0; j < b.N; j++ {
		CalculateFilename(samples[rand.Intn(len(samples))], "/u/batsd")
	}
}

func TestRecordingMetric(t *testing.T) {
	Config.RedisHost = "127.0.0.1"
	Config.RedisPort = 6379

	d := Dispatcher{}
	d.redisPool = &clamp.ConnectionPoolWrapper{}
	d.redisPool.InitPool(redisPoolSize, openRedisConnection)
	r := d.redisPool.GetConnection().(redis.Client)
	defer d.redisPool.ReleaseConnection(r)

	r.Del("datapoints")
	d.RecordMetric("testing")
	if n, _ := r.Scard("datapoints"); n != 1 {
		t.Errorf("Expected 1 datapoint, got %v\n", n)
	}
	if ok, _ := r.Sismember("datapoints", []byte("testing")); !ok {
		t.Errorf("Expected 'testing' to be a member of datapoints; it's not.\n")
	}

}

func TestSavingToRedis(t *testing.T) {
	Config.RedisHost = "127.0.0.1"
	Config.RedisPort = 6379

	d := Dispatcher{}
	d.redisPool = &clamp.ConnectionPoolWrapper{}
	d.redisPool.InitPool(redisPoolSize, openRedisConnection)

	r := d.redisPool.GetConnection().(redis.Client)
	defer d.redisPool.ReleaseConnection(r)

	r.Del("test_metric")
	obs := AggregateObservation{"test_metric", "12345<x>1", 1234, "1"}
	d.writeToRedis(obs)

	if ok, _ := r.Exists("test_metric"); !ok {
		t.Errorf("Metric was not saved.\n")
	}

	if n, _ := r.Zcard("test_metric"); n != 1 {
		t.Errorf("Expected 1 value, got %v\n", n)
	}

	if vals, _ := r.Zrange("test_metric", 0, 1); string(vals[0]) != "12345<x>1" {
		t.Errorf("Expected value to be %v, was %v\n", obs.Content, string(vals[0]))
	}

}

func BenchmarkSavingToRedis(b *testing.B) {
	Config.RedisHost = "127.0.0.1"
	Config.RedisPort = 6379

	d := Dispatcher{}
	d.redisPool = &clamp.ConnectionPoolWrapper{}
	d.redisPool.InitPool(redisPoolSize, openRedisConnection)
	for j := 0; j < b.N; j++ {
		obs := AggregateObservation{"test_metric", "12345<x>1", time.Now().Unix(), "1"}
		d.writeToRedis(obs)
	}
}
