package main

import (
	"bufio"
	"fmt"
	"github.com/noahhl/Go-Redis"
	"github.com/noahhl/go-batsd/gobatsd"
	"io"
	"os"
	"strconv"
	"strings"
)

var timerHeader = map[string]int{"count": 0, "min": 1, "max": 2, "median": 3, "mean": 4,
	"stddev": 5, "percentile_90": 6, "percentile_95": 7, "percentile_99": 8}

var retentions = []int64{60, 600}

func main() {
	gobatsd.LoadConfig()

	spec := redis.DefaultSpec().Host(gobatsd.Config.RedisHost).Port(gobatsd.Config.RedisPort)
	redis, redisErr := redis.NewSynchClientWithSpec(spec)
	if redisErr != nil {
		panic(redisErr)
	}
	datapoints, redisErr := redis.Smembers("datapoints")
	if redisErr != nil {
		panic(redisErr)
	}

	for j := range retentions {
		outfile, err := os.Create(fmt.Sprintf("/u/statsd/counters_export_%v.tsv", retentions[j]))
		if err != nil {
			panic(err)
		}

		writer := bufio.NewWriter(outfile)
		for i := range datapoints {
			if datapoints[i][0] == 'c' {
				path := gobatsd.CalculateFilename(fmt.Sprintf("%v:%v", string(datapoints[i]), retentions[j]), gobatsd.Config.Root)
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
						value, _ := strconv.ParseFloat(parts[1], 64)
						writer.WriteString(fmt.Sprintf("%v\t%v\t%s\n", string(datapoints[i]), parts[0], gobatsd.EncodeFloat64(value)))
					}
				} else {
					fmt.Printf("%v\n", err)
				}
				file.Close()

				writer.Flush()
			}

		}
		outfile.Close()
		fmt.Printf("Import with: -Dimporttsv.columns=HBASE_ROW_KEY,HBASE_TS_KEY,interval%v:value\n", retentions[j])
	}
}
