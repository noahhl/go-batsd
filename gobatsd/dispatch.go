package gobatsd

import (
	"github.com/noahhl/clamp"

	"bufio"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/noahhl/Go-Redis"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

type Dispatcher struct {
	diskChannel  chan AggregateObservation
	redisChannel chan AggregateObservation
	redisPool    *clamp.ConnectionPoolWrapper
}

var numRedisRoutines = 50
var numDiskRoutines = 50
var redisPoolSize = 20

var dispatcher Dispatcher

func SetupDispatcher() {
	dispatcher = Dispatcher{}
	dispatcher.diskChannel = make(chan AggregateObservation, channelBufferSize)
	dispatcher.redisChannel = make(chan AggregateObservation, channelBufferSize)
	dispatcher.redisPool = &clamp.ConnectionPoolWrapper{}
	dispatcher.redisPool.InitPool(redisPoolSize, openRedisConnection)

	for i := 0; i < numDiskRoutines; i++ {
		go func() {
			for {
				obs := <-dispatcher.diskChannel
				dispatcher.writeToDisk(obs)
			}
		}()
	}

	for i := 0; i < numRedisRoutines; i++ {
		go func() {
			for {
				obs := <-dispatcher.redisChannel
				dispatcher.writeToRedis(obs)
			}
		}()
	}
}

func StoreOnDisk(observation AggregateObservation) {
	dispatcher.diskChannel <- observation
}

func StoreInRedis(observation AggregateObservation) {
	dispatcher.redisChannel <- observation
}

func openRedisConnection() (interface{}, error) {
	spec := redis.DefaultSpec().Host(Config.RedisHost).Port(Config.RedisPort)
	r, err := redis.NewSynchClientWithSpec(spec)
	return r, err
}

func (d *Dispatcher) RecordMetric(name string) {
	r := d.redisPool.GetConnection().(redis.Client)
	defer d.redisPool.ReleaseConnection(r)
	r.Sadd("datapoints", []byte(name))
}

func (d *Dispatcher) writeToRedis(observation AggregateObservation) {
	r := d.redisPool.GetConnection().(redis.Client)
	defer d.redisPool.ReleaseConnection(r)
	r.Zadd(observation.Name, float64(observation.Timestamp), []byte(observation.Content))
}

func (d *Dispatcher) writeToDisk(observation AggregateObservation) {
	filename := calculateFilename(observation.Name, Config.Root)

	file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0600)
	newFile := false
	if err != nil {
		if e, ok := err.(*os.PathError); ok && e.Err == syscall.ENOENT {
			fmt.Printf("Creating %v\n", filename)
			//Make containing directories if they don't exist
			err = os.MkdirAll(filepath.Dir(filename), 0755)
			if err != nil {
				fmt.Printf("%v", err)
			}

			file, err = os.Create(filename)
			if err != nil {
				fmt.Printf("%v", err)
			}
			newFile = true
			d.RecordMetric(observation.RawName)
		} else {
			panic(err)
		}
	}
	if file != nil {
		writer := bufio.NewWriter(file)
		if newFile {
			writer.WriteString("v2 " + observation.Name + "\n")
		}
		writer.WriteString(observation.Content)
		writer.Flush()
		file.Close()

	}
}

func calculateFilename(metric string, root string) string {
	h := md5.New()
	io.WriteString(h, metric)
	metricHash := hex.EncodeToString(h.Sum([]byte{}))
	return root + "/" + metricHash[0:2] + "/" + metricHash[2:4] + "/" + metricHash
}