package main

import (
	"bufio"
	"fmt"
	"github.com/jinntrance/goh"
	"github.com/jinntrance/goh/Hbase"
	"github.com/noahhl/Go-Redis"
	"github.com/noahhl/go-batsd/gobatsd"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

var timerHeader = map[string]int{"count": 0, "min": 1, "max": 2, "median": 3, "mean": 4,
	"stddev": 5, "percentile_90": 6, "percentile_95": 7, "percentile_99": 8}

var retentions = []int64{600}

const yearago = 1398249561

func main() {
	gobatsd.LoadConfig()

	runtime.GOMAXPROCS(runtime.NumCPU())
	spec := redis.DefaultSpec().Host(gobatsd.Config.RedisHost).Port(gobatsd.Config.RedisPort)
	redis, redisErr := redis.NewSynchClientWithSpec(spec)
	if redisErr != nil {
		panic(redisErr)
	}
	datapoints, redisErr := redis.Smembers("datapoints")
	if redisErr != nil {
		panic(redisErr)
	}
	nworkers := 10
	c := make(chan int, nworkers)

	for k := 0; k < nworkers; k++ {
		go func(datapoints [][]byte, c chan int) {
			cli, err := gobatsd.OpenHbaseConnection()
			if err != nil {
				panic(err)
			}
			hbaseClient := cli.(*goh.HClient)
			defer hbaseClient.Close()

			for i := range datapoints {
				if datapoints[i][0] == 'g' && (len(datapoints[i]) < 18 || string(datapoints[i][0:17]) != "gauges:Syslog-NG.") {
					path := gobatsd.CalculateFilename(string(datapoints[i]), gobatsd.Config.Root)
					fmt.Printf("(%v / %v) Migrating %v from %v\n", i, len(datapoints), string(datapoints[i]), path)
					file, err := os.Open(path)
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
							if linesRead == 1 {
								continue
							}

							parts := strings.Split(strings.TrimSpace(line), " ")
							ts, _ := strconv.ParseInt(parts[0], 10, 64)
							value, _ := strconv.ParseFloat(parts[1], 64)
							if value != float64(0) && ts > yearago {
								mutations := []*Hbase.Mutation{goh.NewMutation("interval0:value", gobatsd.EncodeFloat64(value))}

								err = hbaseClient.MutateRowTs(gobatsd.Config.HbaseTable, datapoints[i], mutations, ts, nil)
								if err != nil {
									fmt.Printf("%v: mutate error -  %v\n", time.Now(), err)
								}
							}
						}
					} else {
						fmt.Printf("%v\n", err)
					}
					/*} else if datapoints[i][0] == 'c' {
					for j := range retentions {
						path := gobatsd.CalculateFilename(fmt.Sprintf("%v:%v", string(datapoints[i]), retentions[j]), gobatsd.Config.Root)
						fmt.Printf("Migrating %v:%v from %v\n", string(datapoints[i]), retentions[j], path)
						file, err := os.Open(path)
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
								if linesRead == 1 {
									continue
								}

								parts := strings.Split(strings.TrimSpace(line), " ")
								ts, _ := strconv.ParseInt(parts[0], 10, 64)
								value, _ := strconv.ParseFloat(parts[1], 64)

								mutations := []*Hbase.Mutation{goh.NewMutation(fmt.Sprintf("interval%v:value", retentions[j]), gobatsd.EncodeFloat64(value))}

								err = hbaseClient.MutateRowTs(gobatsd.Config.HbaseTable, datapoints[i], mutations, ts, nil)
								if err != nil {
									err = hbaseClient.MutateRowTs(gobatsd.Config.HbaseTable, datapoints[i], mutations, ts, nil)
									if err != nil {
										fmt.Printf("%v: mutate error -  %v\n", time.Now(), err)
									}
								}
							}
						} else {
							fmt.Printf("%v\n", err)
						}

					}*/
				} else if datapoints[i][0] == 't' && (len(datapoints[i]) < 16 || string(datapoints[i][0:15]) != "timers:sysstat.") {
					for j := range retentions {
						path := gobatsd.CalculateFilename(fmt.Sprintf("%v:%v:2", string(datapoints[i]), retentions[j]), gobatsd.Config.Root)
						fmt.Printf("(%v / %v) Migrating %v:%v from %v\n", i, len(datapoints), string(datapoints[i]), retentions[j], path)
						file, err := os.Open(path)
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
								if linesRead == 1 {
									continue
								}

								parts := strings.Split(strings.TrimSpace(line), " ")
								ts, _ := strconv.ParseInt(parts[0], 10, 64)
								timerComponents := strings.Split(parts[1], "/")

								if ts > yearago {
									mutations := make([]*Hbase.Mutation, 0)
									for k, v := range timerHeader {
										val, _ := strconv.ParseFloat(timerComponents[v], 64)
										mutations = append(mutations, goh.NewMutation(fmt.Sprintf("interval%v:%v", retentions[j], k), gobatsd.EncodeFloat64(val)))
									}
									err = hbaseClient.MutateRowTs(gobatsd.Config.HbaseTable, datapoints[i], mutations, ts, nil)
									if err != nil {
										fmt.Printf("%v: mutate error -  %v\n", time.Now(), err)
									}
								}
							}
						} else {
							fmt.Printf("%v\n", err)
						}
					}

				}

			}
			c <- 1
		}(datapoints[k*len(datapoints)/nworkers:(k+1)*len(datapoints)/nworkers-1], c)
	}

	for i := 0; i < nworkers; i++ {
		<-c
	}
}
