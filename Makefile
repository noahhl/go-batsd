
all: build/server

build/server: server/main.go
	go build -o build/go-batsd-server server/main.go

clean:
	rm -f build/*
	make all

