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

var retentions = []int64{60}

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

	for j := range retentions {
		outfile, err := os.Create(fmt.Sprintf("/u/statsd/timers_export_%v.tsv", retentions[j]))
		if err != nil {
			panic(err)
		}

		writer := bufio.NewWriter(outfile)
		for i := range datapoints {
			if datapoints[i][0] == 't' && (len(datapoints[i]) < 16 || string(datapoints[i][0:15]) != "timers:sysstat.") {
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
						if ts > yearago {
							timerComponents := strings.Split(parts[1], "/")
							count, _ := strconv.ParseFloat(timerComponents[0], 64)
							min, _ := strconv.ParseFloat(timerComponents[1], 64)
							max, _ := strconv.ParseFloat(timerComponents[2], 64)
							median, _ := strconv.ParseFloat(timerComponents[3], 64)
							mean, _ := strconv.ParseFloat(timerComponents[4], 64)
							stddev, _ := strconv.ParseFloat(timerComponents[5], 64)
							percentile_90, _ := strconv.ParseFloat(timerComponents[6], 64)
							percentile_95, _ := strconv.ParseFloat(timerComponents[7], 64)
							percentile_99, _ := strconv.ParseFloat(timerComponents[8], 64)

							writer.WriteString(fmt.Sprintf("%v\t%v\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", string(datapoints[i]), parts[0],
								gobatsd.EncodeFloat64(count), gobatsd.EncodeFloat64(min), gobatsd.EncodeFloat64(max), gobatsd.EncodeFloat64(median),
								gobatsd.EncodeFloat64(mean), gobatsd.EncodeFloat64(stddev), gobatsd.EncodeFloat64(percentile_90), gobatsd.EncodeFloat64(percentile_95),
								gobatsd.EncodeFloat64(percentile_99)))
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
		fmt.Printf("Import with: -Dimporttsv.columns=HBASE_ROW_KEY,HBASE_TS_KEY,interval%v:count\n", retentions[j])
	}
}
