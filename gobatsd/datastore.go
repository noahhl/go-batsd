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

type Datastore struct {
	diskChannel     chan AggregateObservation
	redisChannel    chan AggregateObservation
	redisPool       *clamp.ConnectionPoolWrapper
	hashedFilenames map[string]string
}

var numRedisRoutines = 50
var numDiskRoutines = 50
var redisPoolSize = 20
var filenameHashMapSize = 50

var datastore Datastore

func SetupDatastore() {
	datastore = Datastore{}
	datastore.diskChannel = make(chan AggregateObservation, channelBufferSize)
	datastore.redisChannel = make(chan AggregateObservation, channelBufferSize)
	datastore.redisPool = &clamp.ConnectionPoolWrapper{}
	datastore.redisPool.InitPool(redisPoolSize, openRedisConnection)
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
	datastore.hashedFilenames = make(map[string]string, filenameHashMapSize)
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
	filename := d.CalculateFilename(observation.Name, Config.Root)

	file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0600)
	newFile := false
	if err != nil {
		if e, ok := err.(*os.PathError); ok && e.Err == syscall.ENOENT {
			//fmt.Printf("Creating %v\n", filename)
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

func (d *Datastore) CalculateFilename(metric string, root string) string {
	if path, ok := d.hashedFilenames[metric]; ok {
		return path
	} else {
		h := md5.New()
		io.WriteString(h, metric)
		metricHash := hex.EncodeToString(h.Sum([]byte{}))
		path := root + "/" + metricHash[0:2] + "/" + metricHash[2:4] + "/" + metricHash
		d.hashedFilenames[metric] = path
		return path
	}
}