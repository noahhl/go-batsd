all: bin/server

bin/server: server/server.go
	GOPATH=`pwd`/..:${GOPATH} go build -o bin/go-batsd-server server/server.go

clean:
	rm -f bin/*
	make all

package:
	make clean
	fpm -s dir -t deb -n go-batsd -v ${VERSION} --prefix /usr/local/go-batsd bin

