package main

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"redis"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const PORT = 9127
const ROOT = "/u/statsd"

func main() {
	server, err := net.Listen("tcp", ":"+strconv.Itoa(PORT))
	if server == nil {
		panic(err)
	}
	conns := clientConns(server)
	for {
		go handleConn(<-conns)
	}
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

type Datapoint struct {
	Timestamp, Value float64
}

type Retention struct {
	Interval, Count, Duration int64
}

func handleConn(client net.Conn) {
	b := bufio.NewReader(client)
	spec := redis.DefaultSpec()
	redis, redisErr := redis.NewSynchClientWithSpec(spec)

	//FIXME: build from config file
	retentions := []Retention{Retention{10, 360, 10 * 360}, Retention{60, 10080, 60 * 10080}, Retention{600, 52594, 600 * 52594}}

	headers := map[string]int{"count": 0, "min": 1, "max": 2, "median": 3, "mean": 4,
		"stddev": 5, "percentile_90": 6, "percentile_95": 7, "percentile_99": 8,
		"upper_90": 6, "upper_95": 7, "upper_99": 8}

	if redisErr != nil {
		fmt.Printf("Failed to create the client: %v \n", redisErr)
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
			if delta < retentions[0].Duration {

				v, redisErr := redis.Zrangebyscore(metric, startTs, endTs) //metric, start, end
				if redisErr == nil {
					values := make([]Datapoint, len(v))
					for i := 0; i < len(v); i++ {
						parts := strings.Split(string(v[i]), "<X>")
						ts, _ := strconv.ParseFloat(parts[0], 64)
						value := 0.0
						if m, _ := regexp.MatchString("^counters|^gauges", metric); m {
							//counter or gauge - make it a float and move on
							value, _ = strconv.ParseFloat(parts[1], 64)
						} else {
							//timer - find the right index
							timerComponents := strings.Split(parts[1], "/")
							value, _ = strconv.ParseFloat(timerComponents[headers[operation]], 64)
						}
						d := Datapoint{ts, value}
						values[i] = d
					}
					valuesJson, _ := json.Marshal(values)
					client.Write(valuesJson)
					client.Write([]byte("\n"))
				}
			} else {
				//Reading from disk
				retention := retentions[sort.Search(len(retentions), func(i int) bool { return i > 0 && retentions[i].Duration > delta })]
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
				filePath := ROOT + "/" + metricHash[0:2] + "/" + metricHash[2:4] + "/" + metricHash
				file, err := os.Open(filePath)
				if err != nil {
					panic(err)
				}
				reader := bufio.NewReader(file)
				values := make([]Datapoint, 0)
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
					if linesRead == 1 && version == "2" {
						continue
					} //skip the header in v2 files

					parts := strings.Split(strings.TrimSpace(line), " ")
					ts, _ := strconv.ParseFloat(parts[0], 64)
					if ts >= startTs && ts <= endTs {
						value := 0.0
						if m, _ := regexp.MatchString("^counters|^gauges", metric); m {
							//counter or gauge - make it a float and move on
							value, _ = strconv.ParseFloat(parts[1], 64)
						} else {
							//timer - find the right index
							timerComponents := strings.Split(parts[1], "/")
							value, _ = strconv.ParseFloat(timerComponents[headers[operation]], 64)
						}

						d := Datapoint{ts, value}
						l := len(values)
						if l+1 > cap(values) { // reallocate
							newSlice := make([]Datapoint, (l+1)*2)
							copy(newSlice, values)
							values = newSlice
						}
						values = values[0 : l+1]
						values[l] = d
					}
					if ts > endTs {
						break
					}
				}

				file.Close()

				valuesJson, _ := json.Marshal(values)
				client.Write(valuesJson)
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
