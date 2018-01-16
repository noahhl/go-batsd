package gobatsd

import (
	"flag"
	"fmt"
	"github.com/kylelemons/go-gypsy/yaml"
	"path/filepath"
	"strconv"
	"strings"
)

type Retention struct {
	Interval, Count, Duration int64
	Index                     int
}

type Configuration struct {
	Port, Root       string
	Retentions       []Retention
	RedisHost        string
	RedisPort        int
	TargetInterval   int64
	HbaseConnections []string
	HbaseTable       string
	Hbase            bool
	BigtableProject  string
	BigtableInstance string
}

var Config Configuration
var ProfileCPU bool
var channelBufferSize = 10000

func LoadConfig() {
	configPath := flag.String("config", "./config.yml", "config file path")
	port := flag.String("port", "default", "port to bind to")
	duration := flag.Int64("duration", 0, "duration to operation on")
	cpuprofile := flag.Bool("cpuprofile", false, "write cpu profile to file")
	hbase := flag.Bool("hbase", false, "send to hbase")
	hbaseTable := flag.String("table", "statsd", "hbase table to write to")
	flag.Parse()

	absolutePath, _ := filepath.Abs(*configPath)
	c, err := yaml.ReadFile(absolutePath)
	if err != nil {
		panic(err)
	}
	root, _ := c.Get("root")
	if *port == "default" {
		*port, _ = c.Get("port")
	}
	numRetentions, _ := c.Count("retentions")
	retentions := make([]Retention, numRetentions)
	for i := 0; i < numRetentions; i++ {
		retention, _ := c.Get("retentions[" + strconv.Itoa(i) + "]")
		parts := strings.Split(retention, " ")
		d, _ := strconv.ParseInt(parts[0], 0, 64)
		n, _ := strconv.ParseInt(parts[1], 0, 64)
		retentions[i] = Retention{d, n, d * n, i}
	}
	p, _ := c.Get("redis.port")
	redisPort, _ := strconv.Atoi(p)
	redisHost, _ := c.Get("redis.host")

	numHbases, err := c.Count("hbase_hosts")
	if err != nil && *hbase {
		panic(err)
	}
	var hbaseConnections []string
	var bigtable_project, bigtable_instance string
	if *hbase {
		hbaseConnections = make([]string, numHbases)
		for i := 0; i < numHbases; i++ {
			hbaseConnections[i], _ = c.Get("hbase_hosts[" + strconv.Itoa(i) + "]")
		}
		bigtable_project, _ = c.Get("bigtable.project")
		bigtable_instance, _ = c.Get("bigtable.instance")
	}

	Config = Configuration{*port, root, retentions, redisHost, redisPort, *duration, hbaseConnections, *hbaseTable, *hbase, bigtable_project, bigtable_instance}

	ProfileCPU = *cpuprofile
	fmt.Printf("%v\n", Config)
}
