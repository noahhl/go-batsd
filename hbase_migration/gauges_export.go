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

const yearago = 1398249561

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

	outfile, err := os.Create("/u/statsd/gauges_export.tsv")
	if err != nil {
		panic(err)
	}

	writer := bufio.NewWriter(outfile)
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
						writer.WriteString(fmt.Sprintf("%v\t%v\t%s\n", string(datapoints[i]), parts[0], gobatsd.EncodeFloat64(value)))
					}
				}
			} else {
				fmt.Printf("%v\n", err)
			}
			file.Close()

			writer.Flush()
		}

	}
	outfile.Close()
	fmt.Printf("Import with: -Dimporttsv.columns=HBASE_ROW_KEY,HBASE_TS_KEY,interval0:value\n")

}
