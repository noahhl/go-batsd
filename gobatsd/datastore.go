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
	"time"
)

type Datastore struct {
	diskChannel  chan AggregateObservation
	redisChannel chan AggregateObservation
	redisPool    *clamp.ConnectionPoolWrapper
}

var numRedisRoutines = 50
var numDiskRoutines = 100
var redisPoolSize = 20

const diskstoreChannelSize = 100000

var datastore Datastore

func SetupDatastore() {
	datastore = Datastore{}
	datastore.diskChannel = make(chan AggregateObservation, diskstoreChannelSize)
	datastore.redisChannel = make(chan AggregateObservation, channelBufferSize)
	datastore.redisPool = &clamp.ConnectionPoolWrapper{}
	datastore.redisPool.InitPool(redisPoolSize, openRedisConnection)
	go func() {
		c := time.Tick(1 * time.Second)
		for {
			<-c
			clamp.StatsChannel <- clamp.Stat{"datastoreRedisChannelSize", fmt.Sprintf("%v", len(datastore.redisChannel))}
			clamp.StatsChannel <- clamp.Stat{"datastoreDiskChannelSize", fmt.Sprintf("%v", len(datastore.diskChannel))}
		}
	}()
	for i := 0; i < numDiskRoutines; i++ {
		go func() {
			for {
				obs := <-datastore.diskChannel
				datastore.writeToDisk(obs)
			}
		}()
	}

	for i := 0; i < numRedisRoutines; i++ {
		go func() {
			for {
				obs := <-datastore.redisChannel
				datastore.writeToRedis(obs)
			}
		}()
	}
}

func StoreOnDisk(observation AggregateObservation) {
	datastore.diskChannel <- observation
}

func StoreInRedis(observation AggregateObservation) {
	datastore.redisChannel <- observation
}

func openRedisConnection() (interface{}, error) {
	spec := redis.DefaultSpec().Host(Config.RedisHost).Port(Config.RedisPort)
	r, err := redis.NewSynchClientWithSpec(spec)
	return r, err
}

func (d *Datastore) RecordMetric(name string) {
	r := d.redisPool.GetConnection().(redis.Client)
	defer d.redisPool.ReleaseConnection(r)
	r.Sadd("datapoints", []byte(name))
}

func (d *Datastore) writeToRedis(observation AggregateObservation) {
	r := d.redisPool.GetConnection().(redis.Client)
	defer d.redisPool.ReleaseConnection(r)
	r.Zadd(observation.Name, float64(observation.Timestamp), []byte(observation.Content))
}

func (d *Datastore) writeToDisk(observation AggregateObservation) {

	file, err := os.OpenFile(observation.Path, os.O_APPEND|os.O_WRONLY, 0600)
	newFile := false
	if err != nil {
		if e, ok := err.(*os.PathError); ok && e.Err == syscall.ENOENT {
			//fmt.Printf("Creating %v\n", filename)
			//Make containing directories if they don't exist
			err = os.MkdirAll(filepath.Dir(observation.Path), 0755)
			if err != nil {
				fmt.Printf("%v", err)
			}

			file, err = os.Create(observation.Path)
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

func CalculateFilename(metric string, root string) string {
	h := md5.New()
	io.WriteString(h, metric)
	metricHash := hex.EncodeToString(h.Sum([]byte{}))
	return root + "/" + metricHash[0:2] + "/" + metricHash[2:4] + "/" + metricHash
}
