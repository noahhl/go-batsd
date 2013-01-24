package main

import (
	"../shared"
	"fmt"
	"net"
	"os"
	"runtime/pprof"
)

const bufferLen = 100000
const readLen = 256

func main() {
	prof, err := os.Create("/tmp/proxy.prof")
	if err != nil {
		panic(err)
	}
	defer prof.Close()
	pprof.StartCPUProfile(prof)
	defer pprof.StopCPUProfile()

	shared.LoadConfig()
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
	client, err := net.DialUDP("udp", nil, destinationAddr)
	if err != nil {
		panic(err)
	}

	//c := TCPRelay(client)
	buffer := make([]byte, readLen)

	for {
		n, _, err := server.ReadFrom(buffer)
		if err != nil {
			continue
		}
		client.Write(buffer[0:n])
		//	c <- buffer[0:n]

	}
}

func TCPRelay(client net.Conn) chan []byte {
	c := make(chan []byte, bufferLen)

	go func(client net.Conn, c chan []byte) {
		for {
			data := <-c
			client.Write(data)
		}
	}(client, c)

	return c
}
