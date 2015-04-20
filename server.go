package main

import (
	"github.com/noahhl/clamp"
	"github.com/noahhl/go-batsd/gobatsd"

	"bufio"
	"encoding/json"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/jinntrance/goh"
	"github.com/jinntrance/goh/Hbase"
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
	redis redis.Conn
}

var timerHeader = map[string]int{"count": 0, "min": 1, "max": 2, "median": 3, "mean": 4,
	"stddev": 5, "percentile_90": 6, "percentile_95": 7, "percentile_99": 8,
	"upper_90": 6, "upper_95": 7, "upper_99": 8}

var hbasePool *clamp.ConnectionPoolWrapper

func main() {
	gobatsd.LoadConfig()
	fmt.Printf("Starting on port %v, root dir %v\n", gobatsd.Config.Port, gobatsd.Config.Root)

	server, err := net.Listen("tcp", ":"+gobatsd.Config.Port)
	if err != nil {
		panic(err)
	}
	numClients := 0

	hbasePool = gobatsd.MakeHbasePool(10)

	for {
		client, err := server.Accept()
		if client == nil {
			fmt.Printf("couldn't accept: %v", err)
			continue
		}
		numClients++
		fmt.Printf("Opened connection #%d: %v <-> %v\n", numClients, client.LocalAddr(), client.RemoteAddr())

		go func() {
			r, err := redis.Dial("tcp", fmt.Sprintf("%v:%v", gobatsd.Config.RedisHost, gobatsd.Config.RedisPort))
			if err != nil {
				panic(err)
			}
			defer r.Close()

			(&Client{client, r}).Serve()
		}()
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
	available := make([]string, 0)
	smembers, err := c.redis.Do("SMEMBERS", "datapoints")

	if err == nil {
		stringSmembers, _ := redis.Strings(smembers, err)
		for i := range stringSmembers {
			available = append(available, stringSmembers[i])
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
		c.SendValuesFromHbase(metric, 0, start_ts, end_ts, "value")
	case "timers":
		pieces := strings.Split(metric, ":")
		if len(pieces) >= 3 {
			if retention.Index == 0 {
				c.SendValuesFromRedis("timer", "timers:"+pieces[1], start_ts, end_ts, version, pieces[2])
			} else {
				c.SendValuesFromHbase("timers:"+pieces[1], retention.Interval, start_ts, end_ts, pieces[2])
			}
		} else {
			c.Write([]byte("[]\n"))
		}

	case "counters":
		if retention.Index == 0 {
			c.SendValuesFromRedis("counter", metric, start_ts, end_ts, version, "")
		} else {
			c.SendValuesFromHbase(metric, retention.Interval, start_ts, end_ts, "value")
		}

	default:
		c.Write([]byte("Unrecognized datatype\n"))
	}
}

func (c *Client) SendValuesFromRedis(datatype string, keyname string, start_ts float64, end_ts float64,
	version string, operation string) {

	values := make([]map[string]string, 0)
	v, redisErr := c.redis.Do("ZRANGEBYSCORE", keyname, start_ts, end_ts)
	if redisErr == nil {
		stringValues, _ := redis.Strings(v, redisErr)
		for i := range stringValues {
			parts := strings.Split(stringValues[i], "<X>")
			if datatype == "counter" {
				values = append(values, map[string]string{"Timestamp": parts[0], "Value": parts[1]})
			} else if datatype == "timer" {
				if headerIndex, ok := timerHeader[operation]; ok {
					values = append(values, map[string]string{"Timestamp": parts[0], "Value": strings.Split(parts[1], "/")[headerIndex]})
				}
			}

		}
	} else {
		fmt.Printf("%v\n", redisErr)
	}
	json := gobatsd.ArtisinallyMarshallDatapointJSON(values)
	c.Write(append(json, '\n'))
}

func (c *Client) SendValuesFromDisk(datatype string, path string, start_ts float64, end_ts float64,
	version string, operation string) {
	file, err := os.Open(path)
	values := make([]map[string]string, 0)
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
					values = append(values, map[string]string{"Timestamp": parts[0], "Value": parts[1]})
				} else if datatype == "timer" {
					if headerIndex, ok := timerHeader[operation]; ok {
						values = append(values, map[string]string{"Timestamp": parts[0], "Value": strings.Split(parts[1], "/")[headerIndex]})
					}
				}
			}
			if ts > end_ts {
				break
			}
		}
	} else {
		fmt.Printf("%v\n", err)
	}
	json := gobatsd.ArtisinallyMarshallDatapointJSON(values)
	c.Write(append(json, '\n'))

}

func (c *Client) SendValuesFromHbase(keyname string, retentionInterval int64, start_ts float64, end_ts float64, operation string) {
	start := time.Now().UnixNano()
	hbaseClient := hbasePool.GetConnection().(*goh.HClient)

	var result []*Hbase.TCell
	var err error
	if keyname[0] == 'g' { //this is a gauge, so no way to know how many of them there are. get a  bunch
		result, err = hbaseClient.GetVerTs(gobatsd.Config.HbaseTable, []byte(keyname), fmt.Sprintf("interval%v:%v", retentionInterval, operation), int64(end_ts), 1000000, nil)
	} else {
		result, err = hbaseClient.GetVerTs(gobatsd.Config.HbaseTable, []byte(keyname), fmt.Sprintf("interval%v:%v", retentionInterval, operation), int64(end_ts), int32(int64(end_ts-start_ts)/retentionInterval), nil)
	}
	if err != nil {
		fmt.Printf("%v\n", err)
	}
	values := make([]map[string]string, 0)
	for i := range result {

		decodedVal, err := gobatsd.DecodeFloat64(result[i].Value)
		if err == nil {
			values = append(values, map[string]string{"Timestamp": strconv.FormatInt(result[i].Timestamp, 10), "Value": strconv.FormatFloat(decodedVal, 'f', -1, 64)})
		}
	}
	fmt.Printf("Hbase completed request in %v ms, retrieved %v records\n", (time.Now().UnixNano()-start)/int64(time.Millisecond), len(values))

	json := gobatsd.ArtisinallyMarshallDatapointJSON(values)
	c.Write(append(json, '\n'))
	hbasePool.ReleaseConnection(hbaseClient)
	fmt.Printf("Total time to respond to client: %v ms\n", (time.Now().UnixNano()-start)/int64(time.Millisecond))
}
