package main

import (
	"bufio"
	"fmt"
	"github.com/noahhl/Go-Redis"
	"github.com/noahhl/go-batsd/gobatsd"
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
	gobatsd.LoadConfig()
	index := sort.Search(len(gobatsd.Config.Retentions), func(i int) bool { return gobatsd.Config.Retentions[i].Interval == gobatsd.Config.TargetInterval })
	if index == len(gobatsd.Config.Retentions) {
		if gobatsd.Config.Retentions[0].Interval == gobatsd.Config.TargetInterval {
			index = 0
		} else {
			panic("You must specify the duration you'd like to truncate")
		}
	}
	retention := gobatsd.Config.Retentions[index]
	fmt.Printf("Starting truncation for the %v duration.\n", retention.Interval)
	spec := redis.DefaultSpec().Host(gobatsd.Config.RedisHost).Port(gobatsd.Config.RedisPort)
	redis, err := redis.NewSynchClientWithSpec(spec)
	if err != nil {
		panic(err)
	}

	datapoints := make([]string, 0)
	if index == 0 {
		rawDatapoints, err := redis.Keys("*")
		if err != nil {
			panic(err)
		}

		for _, key := range rawDatapoints {
			if m, _ := regexp.MatchString("^timers|^counters", string(key)); m {
				datapoints = append(datapoints, string(key))
			}
		}
	} else {
		rawDatapoints, err := redis.Smembers("datapoints")
		if err != nil {
			panic(err)
		}

		for _, key := range rawDatapoints {
			if m, _ := regexp.MatchString("^timers|^counters", string(key)); m {
				datapoints = append(datapoints, string(key))
			}
		}

	}
	since := float64(time.Now().Unix() - retention.Duration)
	fmt.Printf("Truncating %v datapoints since %f.\n", len(datapoints), since)

	if index == 0 { //Redis truncation
		for _, key := range datapoints {
			redis.Zremrangebyscore(key, 0.0, since)
		}
	} else {
		nworkers := NWORKERS
		if nworkers > len(datapoints) {
			nworkers = len(datapoints)
		}
		c := make(chan int, nworkers)
		for i := 0; i < nworkers; i++ {
			go func(datapoints []string, i int, c chan int) {
				for _, key := range datapoints {
					metricName := key + ":" + strconv.FormatInt(retention.Interval, 10)
					if m, _ := regexp.MatchString("^timers", metricName); m {
						metricName += ":2"
					}
					TruncateOnDisk(metricName, since)
				}
				c <- 1
			}(datapoints[i*len(datapoints)/nworkers:(i+1)*len(datapoints)/nworkers-1], i, c)

		}

		for i := 0; i < nworkers; i++ {
			<-c
		}

	}
}

func TruncateOnDisk(metric string, since float64) {
	filePath := (&gobatsd.Datastore{}).CalculateFilename(metric, gobatsd.Config.Root)
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
