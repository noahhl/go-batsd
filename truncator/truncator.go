package main

import (
	"../shared"
	"bufio"
	"fmt"
	"github.com/noahhl/Go-Redis"
	"io"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const NWORKERS = 4

func main() {
	shared.LoadConfig()
	index := sort.Search(len(shared.Config.Retentions), func(i int) bool { return shared.Config.Retentions[i].Interval == shared.Config.TargetInterval })
	if index == len(shared.Config.Retentions) {
		if shared.Config.Retentions[0].Interval == shared.Config.TargetInterval {
			index = 0
		} else {
			panic("You must specify the duration you'd like to truncate")
		}
	}
	retention := shared.Config.Retentions[index]
	fmt.Printf("Starting truncation for the %v duration.\n", retention.Interval)
	spec := redis.DefaultSpec().Host(shared.Config.RedisHost).Port(shared.Config.RedisPort)
	redis, err := redis.NewSynchClientWithSpec(spec)
	if err != nil {
		panic(err)
	}
	rawDatapoints, err := redis.Smembers("datapoints")
	if err != nil {
		panic(err)
	}
	since := float64(time.Now().Unix() - retention.Duration)
	datapoints := make([]string, 0)
	for _, key := range rawDatapoints {
		if m, _ := regexp.MatchString("^gauges", string(key)); !m {
			l := len(datapoints)
			if l+1 > cap(datapoints) {
				newSlice := make([]string, (l+1)*2)
				copy(newSlice, datapoints)
				datapoints = newSlice
			}
			datapoints = datapoints[0 : l+1]
			datapoints[l] = string(key)
		}

	}
	fmt.Printf("Truncating %v datapoints since %f.\n", len(datapoints), since)

	if index == 0 { //Redis truncation
		for _, key := range datapoints {
			redis.Zremrangebyscore(key, 0.0, since)
		}
	} else {
		c := make(chan int, NWORKERS)
		for i := 0; i < NWORKERS; i++ {
			go func(datapoints []string, i int, c chan int) {
				for _, key := range datapoints {
					metricName := key + ":" + strconv.FormatInt(retention.Interval, 10)
					if m, _ := regexp.MatchString("^timers", metricName); m {
						metricName += ":2"
					}
					TruncateOnDisk(metricName, since)
				}
				c <- 1
			}(datapoints[i*len(datapoints)/NWORKERS:(i+1)*len(datapoints)/NWORKERS-1], i, c)

		}

		for i := 0; i < NWORKERS; i++ {
			<-c
		}

	}
}

func TruncateOnDisk(metric string, since float64) {
	filePath := shared.CalculateFilename(metric, shared.Config.Root)
	file, err := os.Open(filePath)
	tmpfile, writeErr := os.Create(filePath + "tmp")
	if writeErr != nil {
		panic(writeErr)
	}
	if err == nil && writeErr == nil {
		linesWritten := 0
		reader := bufio.NewReader(file)
		writer := bufio.NewWriter(tmpfile)
		for {
			line, err := reader.ReadString('\n')
			if err != nil && err != io.EOF {
				panic(err)
			}
			if err != nil && err == io.EOF {
				break
			}
			//skip the header in v2 files
			if line[0] == 'v' {
				writer.WriteString(line)
			} else {

				parts := strings.Split(strings.TrimSpace(line), " ")
				ts, _ := strconv.ParseFloat(parts[0], 64)
				if ts >= since {
					writer.WriteString(line)
					linesWritten++
				}
			}
		}
		writer.Flush()
		file.Close()
		tmpfile.Close()
		if linesWritten > 0 {
			os.Rename(filePath+"tmp", filePath)
		} else {
			os.Remove(filePath + "tmp")
			os.Remove(filePath)
		}
	}

}