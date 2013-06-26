all: bin/server bin/truncator bin/proxy bin/receiver

bin/server: server.go
	go build -o bin/go-batsd-server server.go

bin/receiver: receiver.go
	go build -o bin/go-batsd-receiver receiver.go

bin/truncator: truncator.go
	go build -o bin/go-batsd-truncator truncator.go

bin/proxy: proxy.go
	go build -o bin/go-batsd-proxy proxy.go

clean:
	rm -f bin/*
	make all

package:
	make clean
	fpm -s dir -t deb -n go-batsd -v ${VERSION} --prefix /usr/local/go-batsd bin

