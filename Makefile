
all: bin/server

bin/server: server/main.go
	go build -o bin/go-batsd-server server/main.go

clean:
	rm -f bin/*
	make all

package:
	make clean
	fpm -s dir -t deb -n go-batsd -v ${VERSION} --prefix /usr/lib/go-batsd bin

