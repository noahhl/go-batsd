package gobatsd

import (
	"github.com/noahhl/clamp"

	"bufio"
	"cloud.google.com/go/bigtable"
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/jinntrance/goh"
	"github.com/jinntrance/goh/Hbase"
	"github.com/noahhl/Go-Redis"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

type Datastore struct {
	diskChannel   chan AggregateObservation
	redisChannel  chan AggregateObservation
	hbaseChannel  chan AggregateObservation
	redisPool     *clamp.ConnectionPoolWrapper
	hbasePool     *clamp.ConnectionPoolWrapper
	bigtableTable *bigtable.Table
}

var numRedisRoutines = 50
var numDiskRoutines = 25

var numHbaseRoutines = 100

const hbaseBatchSize = 100

var redisPoolSize = 20

const diskstoreChannelSize = 100000

var datastore Datastore

func SetupDatastore() {
	datastore = Datastore{}
	datastore.diskChannel = make(chan AggregateObservation, diskstoreChannelSize)
	datastore.redisChannel = make(chan AggregateObservation, channelBufferSize)
	datastore.redisPool = MakeRedisPool(redisPoolSize)

	if Config.Hbase {
		datastore.hbaseChannel = make(chan AggregateObservation, channelBufferSize)
		datastore.hbasePool = MakeHbasePool(numHbaseRoutines)
		jsonKey, err := ioutil.ReadFile("/etc/secrets/service_account_key.json")
		if err != nil {
			panic(err)
		}
		config, err := google.JWTConfigFromJSON(jsonKey, bigtable.Scope)
		if err != nil {
			panic(err)
		}
		client, err := bigtable.NewClient(context.Background(), Config.BigtableProject, Config.BigtableInstance, option.WithTokenSource(config.TokenSource(context.Background())))
		if err != nil {
			panic(err)
		}

		datastore.bigtableTable = client.Open(strings.Replace(Config.HbaseTable, ":", "-", -1))
	}

	go func() {
		c := time.Tick(1 * time.Second)
		for {
			<-c
			clamp.StatsChannel <- clamp.Stat{"datastoreRedisChannelSize", fmt.Sprintf("%v", len(datastore.redisChannel))}
			clamp.StatsChannel <- clamp.Stat{"datastoreDiskChannelSize", fmt.Sprintf("%v", len(datastore.diskChannel))}
			if Config.Hbase {
				clamp.StatsChannel <- clamp.Stat{"datastoreHbaseChannelSize", fmt.Sprintf("%v", len(datastore.hbaseChannel))}
			}
		}
	}()

	for i := 0; i < numHbaseRoutines; i++ {
		go func() {
			observations := make([]AggregateObservation, 0)
			for {
				obs := <-datastore.diskChannel
				observations = append(observations, obs)
				if len(observations) >= hbaseBatchSize {
					sort.Sort(AggregateObservations(observations))
					batchStart := 0
					for k := 1; k < len(observations); k++ {
						if observations[k].Timestamp != observations[k-1].Timestamp {
							datastore.writeToHbaseInBulk(observations[batchStart:k])
							batchStart = k
						}
					}
					datastore.writeToHbaseInBulk(observations[batchStart:len(observations)])

					observations = make([]AggregateObservation, 0)
				}
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

func MakeRedisPool(size int) *clamp.ConnectionPoolWrapper {
	pool := &clamp.ConnectionPoolWrapper{}
	pool.InitPool(size, openRedisConnection)
	return pool
}

func MakeHbasePool(size int) *clamp.ConnectionPoolWrapper {
	pool := &clamp.ConnectionPoolWrapper{}
	pool.InitPool(size, OpenHbaseConnection)
	return pool
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

func OpenHbaseConnection() (interface{}, error) {
	host := Config.HbaseConnections[rand.Intn(len(Config.HbaseConnections))]
	fmt.Printf("%v: Opening an hbase connection to %v\n", time.Now(), host)
	hbaseClient, err := goh.NewTcpClient(host, goh.TBinaryProtocol, false, 10*time.Second)
	if err != nil {
		fmt.Println(err)
	}
	err = hbaseClient.Open()
	if err != nil {
		fmt.Println(err)
	}
	return hbaseClient, err
}

func (d *Datastore) RecordMetric(name string) {
	r := d.redisPool.GetConnection().(redis.Client)
	defer d.redisPool.ReleaseConnection(r)
	r.Sadd("datapoints", []byte(name))
}

func (d *Datastore) RecordCurrent(name string, val float64) {
	r := d.redisPool.GetConnection().(redis.Client)
	defer d.redisPool.ReleaseConnection(r)
	r.Set(name, EncodeFloat64(val))
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
				fmt.Printf("%v\n", err)
			}

			file, err = os.Create(observation.Path)
			if err != nil {
				fmt.Printf("%v\n", err)
			}
			newFile = true
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
func (d *Datastore) writeToBigtableInBulk(observations []AggregateObservation) {
	rowNames := make([]string, 0)
	mutations := make([]*bigtable.Mutation, 0)
	for i := range observations {
		mut := bigtable.NewMutation()
		for k, v := range observations[i].SummaryValues {
			mut.Set(fmt.Sprintf("interval%v", observations[i].Interval), k, bigtable.Timestamp(observations[i].Timestamp*1000), EncodeFloat64(v))
		}

		mutations = append(mutations, mut)
		rowNames = append(rowNames, observations[i].RawName)
	}
	rowErrors, err := d.bigtableTable.ApplyBulk(context.Background(), rowNames, mutations)
	if err != nil {
		fmt.Printf("ApplyBulk error: %v\n", err)
	}
	for _, e := range rowErrors {
		fmt.Printf("Row error: %v", e)
	}
}

func (d *Datastore) writeToHbaseInBulk(observations []AggregateObservation) {
	d.writeToBigtableInBulk(observations)
	c := d.hbasePool.GetConnection().(*goh.HClient)
	bulk := make([]*Hbase.BatchMutation, len(observations))
	for i := range observations {
		mutations := make([]*Hbase.Mutation, 0)
		for k, v := range observations[i].SummaryValues {
			mutations = append(mutations, goh.NewMutation(fmt.Sprintf("interval%v:%v", observations[i].Interval, k), EncodeFloat64(v)))
		}
		bulk[i] = goh.NewBatchMutation([]byte(observations[i].RawName), mutations)

	}
	err := c.MutateRowsTs(Config.HbaseTable, bulk, observations[0].Timestamp, nil)
	if err != nil {
		fmt.Printf("%v: mutate error using %v -  %v, reconnecting\n", time.Now(), c, err)
		x, _ := OpenHbaseConnection()
		c = x.(*goh.HClient)
	}
	d.hbasePool.ReleaseConnection(c)
}

func CalculateFilename(metric string, root string) string {
	h := md5.New()
	io.WriteString(h, metric)
	metricHash := hex.EncodeToString(h.Sum([]byte{}))
	return root + "/" + metricHash[0:2] + "/" + metricHash[2:4] + "/" + metricHash
}

func EncodeFloat64(float float64) []byte {
	bits := math.Float64bits(float)
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, bits)
	return bytes
}

func DecodeFloat64(b []byte) (float64, error) {
	if len(b) == 8 {
		bits := binary.BigEndian.Uint64(b)
		return math.Float64frombits(bits), nil
	}
	err := errors.New("Incorrect number of bits for a float64")
	return 0, *&err
}
