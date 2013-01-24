package shared

import (
	"flag"
	"github.com/kylelemons/go-gypsy/yaml"
	"path/filepath"
	"strconv"
	"strings"
)

type Retention struct {
	Interval, Count, Duration int64
}

type Configuration struct {
	Port, Root     string
	Retentions     []Retention
	RedisHost      string
	RedisPort      int
	TargetInterval int64
}

var Config Configuration
var ProfileCPU bool

func LoadConfig() {
	configPath := flag.String("config", "./config.yml", "config file path")
	port := flag.String("port", "default", "port to bind to")
	duration := flag.Int64("duration", 0, "duration to operation on")
	cpuprofile := flag.Bool("cpuprofile", false, "write cpu profile to file")
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
		retentions[i] = Retention{d, n, d * n}
	}
	p, _ := c.Get("redis.port")
	redisPort, _ := strconv.Atoi(p)
	redisHost, _ := c.Get("redis.host")
	Config = Configuration{*port, root, retentions, redisHost, redisPort, *duration}

	ProfileCPU = *cpuprofile
}
