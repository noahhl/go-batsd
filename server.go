package main

import (
	"github.com/noahhl/go-batsd/gobatsd"

	"bufio"
	"encoding/json"
	"fmt"
	"github.com/noahhl/Go-Redis"
	"github.com/noahhl/clamp"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	net.Conn
}

var redisPool *clamp.ConnectionPoolWrapper

var timerHeader = map[string]int{"count": 0, "min": 1, "max": 2, "median": 3, "mean": 4,
	"stddev": 5, "percentile_90": 6, "percentile_95": 7, "percentile_99": 8,
	"upper_90": 6, "upper_95": 7, "upper_99": 8}

func main() {
	gobatsd.LoadConfig()
	fmt.Printf("Starting on port %v, root dir %v\n", gobatsd.Config.Port, gobatsd.Config.Root)

	redisPool = gobatsd.MakeRedisPool(10)

	server, err := net.Listen("tcp", ":"+gobatsd.Config.Port)
	if err != nil {
		panic(err)
	}
	numClients := 0
	for {
		client, err := server.Accept()
		if client == nil {
			fmt.Printf("couldn't accept: %v", err)
			continue
		}
		numClients++
		fmt.Printf("Opened connection #%d: %v <-> %v\n", numClients, client.LocalAddr(), client.RemoteAddr())
		go (&Client{client}).Serve()
	}
}

func (c *Client) Serve() {
	for {
		b := bufio.NewReader(c)
		line, err := b.ReadBytes('\n')
		if err != nil {
			break
		}
		components := strings.Split(strings.TrimSpace(string(line)), " ")
		command := strings.ToLower(components[0])
		switch command {
		case "ping":
			c.Write([]byte("PONG\n"))
		case "quit":
			c.Write([]byte("BYE\n"))
			c.Close()
		case "available":
			c.SendAvailableMetrics()
		case "values":
			c.SendValues(components[1:len(components)])
		default:
			c.Write([]byte("Unrecognized command: " + command + "\n"))
		}
	}
}

func (c *Client) SendAvailableMetrics() {
	r := redisPool.GetConnection().(redis.Client)
	defer redisPool.ReleaseConnection(r)

	available := make([]string, 0)
	smembers, err := r.Smembers("datapoints")

	if err == nil {
		for i := range smembers {
			available = append(available, string(smembers[i]))
		}
	}
	json, _ := json.Marshal(available)
	c.Write(append(json, '\n'))

}

func (c *Client) SendValues(properties []string) {
	if len(properties) < 3 {
		c.Write([]byte("Invalid arguments\n"))
		return
	}
	metric := properties[0]
	start_ts, _ := strconv.ParseFloat(properties[1], 64)
	end_ts, _ := strconv.ParseFloat(properties[2], 64)
	version := ":2"
	if len(properties) == 4 && properties[3] == "1" {
		version = ""
	}

	datatype := strings.Split(metric, ":")[0]
	now := time.Now().Unix()
	retentionIndex := sort.Search(len(gobatsd.Config.Retentions), func(i int) bool { return gobatsd.Config.Retentions[i].Duration > (now - int64(start_ts)) })
	if retentionIndex == len(gobatsd.Config.Retentions) {
		retentionIndex = len(gobatsd.Config.Retentions) - 1
	}
	retention := gobatsd.Config.Retentions[retentionIndex]
	switch datatype {
	case "gauges":
		c.SendValuesFromDisk("gauge", gobatsd.CalculateFilename(metric, gobatsd.Config.Root),
			start_ts, end_ts, version, "")

	case "timers":
		pieces := strings.Split(metric, ":")
		if retention.Index == 0 {
			c.SendValuesFromRedis("timer", "timers:"+pieces[1], start_ts, end_ts, version, pieces[2])
		} else {
			c.SendValuesFromDisk("timer", gobatsd.CalculateFilename(fmt.Sprintf("timers:%v:%v%v", pieces[1], retention.Interval, version), gobatsd.Config.Root),
				start_ts, end_ts, version, pieces[2])
		}

	case "counters":
		if retention.Index == 0 {
			c.SendValuesFromRedis("counter", metric, start_ts, end_ts, version, "")
		} else {
			c.SendValuesFromDisk("counter", gobatsd.CalculateFilename(fmt.Sprintf("%v:%v", metric, retention.Interval), gobatsd.Config.Root),
				start_ts, end_ts, version, "")
		}

	default:
		c.Write([]byte("Unrecognized datatype\n"))
	}
}

func (c *Client) SendValuesFromRedis(datatype string, keyname string, start_ts float64, end_ts float64,
	version string, operation string) {
	r := redisPool.GetConnection().(redis.Client)
	defer redisPool.ReleaseConnection(r)

	values := make([]map[string]float64, 0)
	v, redisErr := r.Zrangebyscore(keyname, start_ts, end_ts)
	if redisErr == nil {
		for i := range v {
			parts := strings.Split(string(v[i]), "<X>")
			ts, _ := strconv.ParseFloat(parts[0], 64)
			if datatype == "counter" {
				value, _ := strconv.ParseFloat(parts[1], 64)
				values = append(values, map[string]float64{"Timestamp": ts, "Value": value})
			} else if datatype == "timer" {
				if headerIndex, ok := timerHeader[operation]; ok {
					value, _ := strconv.ParseFloat(strings.Split(parts[1], "/")[headerIndex], 64)
					values = append(values, map[string]float64{"Timestamp": ts, "Value": value})
				}
			}

		}
	}
	json, _ := json.Marshal(values)
	c.Write(append(json, '\n'))
}

func (c *Client) SendValuesFromDisk(datatype string, path string, start_ts float64, end_ts float64,
	version string, operation string) {
	file, err := os.Open(path)
	values := make([]map[string]float64, 0)
	if err == nil {
		reader := bufio.NewReader(file)
		linesRead := 0
		for {
			line, err := reader.ReadString('\n')
			linesRead += 1
			if err != nil && err != io.EOF {
				panic(err)
			}
			if err != nil && err == io.EOF {
				break
			}
			//skip the header in v2 files
			if linesRead == 1 && version == ":2" {
				continue
			}

			parts := strings.Split(strings.TrimSpace(line), " ")
			ts, _ := strconv.ParseFloat(parts[0], 64)
			if ts >= start_ts && ts <= end_ts {
				if datatype == "gauge" || datatype == "counter" {
					value, _ := strconv.ParseFloat(parts[1], 64)
					values = append(values, map[string]float64{"Timestamp": ts, "Value": value})
				} else if datatype == "timer" {
					if headerIndex, ok := timerHeader[operation]; ok {
						value, _ := strconv.ParseFloat(strings.Split(parts[1], "/")[headerIndex], 64)
						values = append(values, map[string]float64{"Timestamp": ts, "Value": value})
					}
				}
			}
			if ts > end_ts {
				break
			}
		}
	}
	json, _ := json.Marshal(values)
	c.Write(append(json, '\n'))

}
