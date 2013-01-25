package main

import (
	"../shared"
	"bufio"
	"fmt"
	"net"
	"os"
	"regexp"
	"strconv"
	"syscall"
	"time"
)

type Datapoint struct {
	Timestamp time.Time
	Name      string
	Value     float64
	Datatype  string
}

var gaugeChannel chan Datapoint
var client net.Conn

const readLen = 256

func main() {
	shared.LoadConfig()
	gaugeChannel = make(chan Datapoint)

	fmt.Printf("Starting on port %v\n", shared.Config.Port)

	server, err := net.ListenPacket("udp", ":"+shared.Config.Port)
	defer server.Close()
	if err != nil {
		panic(err)
	}

	destinationAddr, err := net.ResolveUDPAddr("udp", ":8225")
	if err != nil {
		panic(err)
	}
	client, err = net.DialUDP("udp", nil, destinationAddr)
	if err != nil {
		panic(err)
	}

	go HandleGauge(gaugeChannel)

	buffer := make([]byte, readLen)
	for {
		n, _, err := server.ReadFrom(buffer)
		if err != nil {
			continue
		}
		ProcessIncomingMessage(string(buffer[0:n]))
	}

}

func ProcessIncomingMessage(message string) {
	d := ParseDatapoint(message)
	if d.Datatype == "g" {
		gaugeChannel <- d
	} else if d.Datatype == "c" {
		client.Write([]byte(message))
	} else if d.Datatype == "ms" {
		client.Write([]byte(message))
	}

}

func ParseDatapoint(metric string) Datapoint {
	metricRegex, err := regexp.Compile("(.*):([0-9|\\.]+)\\|(c|g|ms)")
	if err != nil {
		panic(err)
	}
	matches := metricRegex.FindAllStringSubmatch(metric, -1)
	d := Datapoint{}
	if len(matches) > 0 && len(matches[0]) == 4 {
		value, _ := strconv.ParseFloat(matches[0][2], 64)
		d = Datapoint{time.Now(), matches[0][1], value, matches[0][3]}
	}
	return d
}

func HandleGauge(ch chan Datapoint) {
	for {
		d := <-ch
		//fmt.Printf("Processing gauge %v with value %v and timestamp %v \n", d.Name, d.Value, d.Timestamp)
		filename := shared.CalculateFilename("gauges:"+d.Name, shared.Config.Root)

		file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0600)
		newFile := false
		if err != nil {
			if e, ok := err.(*os.PathError); ok && e.Err == syscall.ENOENT {
				fmt.Printf("Creating %v\n", filename)
				file, err = os.Create(filename)
				if err != nil {
					panic(err)
				}
				newFile = true
			} else {
				panic(err)
			}
		}
		writer := bufio.NewWriter(file)
		if newFile {
			writer.WriteString("v2 gauges:" + d.Name + "\n")
		}
		writer.WriteString(fmt.Sprintf("%d %v\n", d.Timestamp.Unix(), d.Value))
		writer.Flush()
		file.Close()

	}
}
