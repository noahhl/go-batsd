package main

import (
	"../shared"
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/noahhl/Go-Redis"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Datapoint struct {
	Timestamp, Value float64
}

func main() {

	shared.LoadConfig()
	fmt.Printf("Starting on port %v, root dir %v\n", shared.Config.Port, shared.Config.Root)
	server, err := net.Listen("tcp", ":"+shared.Config.Port)
	if err != nil {
		panic(err)
	}
	conns := clientConns(server)
	for {
		go handleConn(<-conns)
	}
}

func metricIsX(metric string, x string) (result bool) {
	if strings.HasPrefix(metric, x) {
		result = true
	}
	return
}

func metricIsCounter(metric string) bool {
	return metricIsX(metric, "counters")
}

func metricIsGuage(metric string) bool {
	return metricIsX(metric, "guages")
}

func metricIsTimer(metric string) bool {
	return metricIsX(metric, "timers")
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
	if metricIsCounter(metric) || metricIsGuage(metric) {
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
	spec := redis.DefaultSpec().Host(shared.Config.RedisHost).Port(shared.Config.RedisPort)
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

		if strings.Contains(strings.ToLower(string(line)), "available") {
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
		} else if strings.Contains("values", string(line)) {
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

			if !(metricIsCounter(metric) || metricIsGuage(metric)) {
				pieces := strings.Split(metric, ":")
				if len(pieces) >= 3 {
					operation = strings.Split(metric, ":")[2]
				}
				metric = strings.Replace(metric, ":"+operation, "", -1)
			}

			startTs, _ := strconv.ParseFloat(parts[2], 64)
			endTs, _ := strconv.ParseFloat(parts[3], 64)

			//Redis retention
			if !metricIsGuage(metric) && delta < shared.Config.Retentions[0].Duration {

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
				retention := shared.Config.Retentions[sort.Search(len(shared.Config.Retentions), func(i int) bool { return i > 0 && shared.Config.Retentions[i].Duration > delta })]
				if metricIsTimer(metric) {
					if version == "2" {
						metric = metric + ":" + strconv.FormatInt(retention.Interval, 10) + ":2"
					} else {
						metric = metric + ":" + operation + ":" + strconv.FormatInt(retention.Interval, 10)
					}
				} else if metricIsCounter(metric) {
					metric = metric + ":" + strconv.FormatInt(retention.Interval, 10)
				}
				filePath := shared.CalculateFilename(metric, shared.Config.Root)
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
		} else if strings.Contains(strings.ToLower(string(line)), "ping") {
			client.Write([]byte("PONG\n"))
		} else if strings.Contains(strings.ToLower(string(line)), "quit") || strings.Contains(strings.ToLower(string(line)), "exit") {
			client.Write([]byte("BYE\n"))
			client.Close()
		} else {
			resp, _ := json.Marshal("Unrecognized command: " + string(line))
			client.Write(resp)
			client.Write([]byte("\n"))
		}
	}
}
