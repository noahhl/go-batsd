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

const yearago = 1398249561

var maxPerFile = 100000

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

	fileNo := 1
	outfile, err := os.Create(fmt.Sprintf("gauges_export-%v.tsv", fileNo))
	if err != nil {
		panic(err)
	}

	writer := bufio.NewWriter(outfile)
	for i := range datapoints {
		if i == fileNo*maxPerFile {
			outfile.Close()
			fileNo += 1
			outfile, err = os.Create(fmt.Sprintf("gauges_export-%v.tsv", fileNo))
			if err != nil {
				panic(err)
			}
			writer = bufio.NewWriter(outfile)
		}
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
						fmt.Println(err)
					}
					if err != nil && err == io.EOF {
						break
					}
					if linesRead == 1 {
						continue
					}

					parts := strings.Split(strings.TrimSpace(line), " ")
					ts, _ := strconv.ParseInt(parts[0], 10, 64)
					if len(parts) == 2 {
						value, _ := strconv.ParseFloat(parts[1], 64)
						if value != float64(0) && ts > yearago {
							_, err := writer.WriteString(fmt.Sprintf("%v\t%v\t%s\n", string(datapoints[i]), parts[0], gobatsd.EncodeFloat64(value)))
							if err != nil {
								fmt.Println(err)
								outfile.Close()
								fileNo += 1
								outfile, err = os.Create(fmt.Sprintf("sc_gauges_export-%v.tsv", fileNo))
								if err != nil {
									panic(err)
								}
								writer = bufio.NewWriter(outfile)

							}
						}
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
