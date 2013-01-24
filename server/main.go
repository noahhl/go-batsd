package main

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/kylelemons/go-gypsy/yaml"
	"github.com/noahhl/Go-Redis"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Datapoint struct {
	Timestamp, Value float64
}

type Retention struct {
	Interval, Count, Duration int64
}

type Config struct {
	port, root string
	retentions []Retention
	redisHost  string
	redisPort  int
}

var config Config

func main() {

	loadConfig()
	server, err := net.Listen("tcp", ":"+config.port)
	if err != nil {
		panic(err)
	}
	conns := clientConns(server)
	for {
		go handleConn(<-conns)
	}
}

func loadConfig() {
	configPath := flag.String("config", "./config.yml", "config file path")
	port := flag.String("port", "default", "port to bind to")
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
	config = Config{*port, root, retentions, redisHost, redisPort}
	fmt.Printf("Starting on port %v, root dir %v\n", config.port, config.root)
}

func clientConns(listener net.Listener) chan net.Conn {
	ch := make(chan net.Conn)
	i := 0
	go func() {
		for {
			client, err := listener.Accept()
			if client == nil {
				fmt.Printf("couldn't accept: %v", err)
				continue
			}
			i++
			fmt.Printf("Opened connection #%d: %v <-> %v\n", i, client.LocalAddr(), client.RemoteAddr())
			ch <- client
		}
	}()
	return ch
}

func createDatapoint(rawTs string, rawValue string, operation string, metric string, version string) Datapoint {

	headers := map[string]int{"count": 0, "min": 1, "max": 2, "median": 3, "mean": 4,
		"stddev": 5, "percentile_90": 6, "percentile_95": 7, "percentile_99": 8,
		"upper_90": 6, "upper_95": 7, "upper_99": 8}
	ts, _ := strconv.ParseFloat(rawTs, 64)
	value := 0.0
	if m, _ := regexp.MatchString("^counters|^gauges", metric); m {
		//counter or gauge - make it a float and move on
		value, _ = strconv.ParseFloat(rawValue, 64)
	} else {
		//timer - find the right index
		timerComponents := strings.Split(rawValue, "/")
		value, _ = strconv.ParseFloat(timerComponents[headers[operation]], 64)
	}
	d := Datapoint{ts, value}
	return d
}

func serializeDatapoints(datapoints []Datapoint) []byte {

	valuesJson, _ := json.Marshal(datapoints)
	return valuesJson
}

func handleConn(client net.Conn) {
	b := bufio.NewReader(client)
	spec := redis.DefaultSpec().Host(config.redisHost).Port(config.redisPort)
	redis, redisErr := redis.NewSynchClientWithSpec(spec)

	if redisErr != nil {
		fmt.Printf("Failed to create the Redis client: %v \n", redisErr)
		client.Close()
		return
	}

	for {
		line, err := b.ReadBytes('\n')
		if err != nil {
			break
		}

		if m, _ := regexp.MatchString("(?i)available", string(line)); m {
			a, redisErr := redis.Smembers("datapoints")
			available := make([]string, len(a))
			for i := 0; i < len(a); i++ {
				available[i] = string(a[i])
			}
			if redisErr != nil {
				fmt.Printf("Redis error: %v \n", redisErr)
			} else {
				availableJSON, err := json.Marshal(available)
				if err == nil {
					client.Write(availableJSON)
					client.Write([]byte("\n"))
				}
			}
		} else if m, _ := regexp.MatchString("values", string(line)); m {
			parts := strings.Split(strings.TrimSpace(string(line)), " ")
			if len(parts) < 3 {
				client.Write([]byte("Invalid arguments"))
				break
			}
			version := "2"
			if len(parts) > 4 {
				version = parts[4]
			}

			now := time.Now().Unix()
			beginTime, _ := strconv.ParseInt(parts[2], 0, 64)
			delta := now - beginTime
			metric := parts[1]
			operation := ""
			if m, _ := regexp.MatchString("^counters|^gauges", metric); !m {
				operation = strings.Split(metric, ":")[2]
				metric = strings.Replace(metric, ":"+operation, "", -1)
			}

			startTs, _ := strconv.ParseFloat(parts[2], 64)
			endTs, _ := strconv.ParseFloat(parts[3], 64)

			//Redis retention
			if delta < config.retentions[0].Duration {

				v, redisErr := redis.Zrangebyscore(metric, startTs, endTs) //metric, start, end
				if redisErr == nil {
					values := make([]Datapoint, len(v))
					for i := 0; i < len(v); i++ {
						parts := strings.Split(string(v[i]), "<X>")
						values[i] = createDatapoint(parts[0], parts[1], operation, metric, version)
					}
					client.Write(serializeDatapoints(values))
					client.Write([]byte("\n"))
				}
			} else {
				//Reading from disk
				retention := config.retentions[sort.Search(len(config.retentions), func(i int) bool { return i > 0 && config.retentions[i].Duration > delta })]
				if m, _ := regexp.MatchString("^timers", metric); m {
					if version == "2" {
						metric = metric + ":" + strconv.FormatInt(retention.Interval, 10) + ":2"
					} else {
						metric = metric + ":" + operation + ":" + strconv.FormatInt(retention.Interval, 10)
					}
				} else {
					metric = metric + ":" + strconv.FormatInt(retention.Interval, 10)
				}
				h := md5.New()
				io.WriteString(h, metric)
				metricHash := hex.EncodeToString(h.Sum([]byte{}))
				filePath := config.root + "/" + metricHash[0:2] + "/" + metricHash[2:4] + "/" + metricHash
				file, err := os.Open(filePath)
				values := make([]Datapoint, 0)
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
						if linesRead == 1 && version == "2" {
							continue
						}

						parts := strings.Split(strings.TrimSpace(line), " ")
						ts, _ := strconv.ParseFloat(parts[0], 64)
						if ts >= startTs && ts <= endTs {
							l := len(values)
							if l+1 > cap(values) { // reallocate
								newSlice := make([]Datapoint, (l+1)*2)
								copy(newSlice, values)
								values = newSlice
							}
							values = values[0 : l+1]
							values[l] = createDatapoint(parts[0], parts[1], operation, metric, version)
						}
						if ts > endTs {
							break
						}
					}

					file.Close()
				}

				client.Write(serializeDatapoints(values))
				client.Write([]byte("\n"))
			}

		} else if m, _ := regexp.MatchString("(?i)ping", string(line)); m {
			client.Write([]byte("PONG\n"))
		} else if m, _ := regexp.MatchString("(?i)quit|exit", string(line)); m {
			client.Write([]byte("BYE\n"))
			client.Close()
		} else {
			resp, _ := json.Marshal("Unrecognized command: " + string(line))
			client.Write(resp)
			client.Write([]byte("\n"))
		}
	}
}
