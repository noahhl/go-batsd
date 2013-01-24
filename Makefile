all: bin/server bin/truncator bin/proxy 

bin/server: server/server.go
	go build -o bin/go-batsd-server server/server.go

bin/truncator: truncator/truncator.go
	go build -o bin/go-batsd-truncator truncator/truncator.go

bin/proxy: proxy/proxy.go
	go build -o bin/go-batsd-proxy proxy/proxy.go

clean:
	rm -f bin/*
	make all

package:
	make clean
	fpm -s dir -t deb -n go-batsd -v ${VERSION} --prefix /usr/local/go-batsd bin

