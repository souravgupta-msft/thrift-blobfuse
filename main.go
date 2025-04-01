package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"thrift-blobfuse/client"
	"thrift-blobfuse/server"

	"github.com/apache/thrift/lib/go/thrift"
)

func main() {
	isServer := flag.Bool("server", false, "Run server")
	addr := flag.String("addr", "localhost:9090", "Address to listen to")
	secure := flag.Bool("secure", false, "Use tls secure transport")

	flag.Parse()

	var protocolFactory thrift.TProtocolFactory = thrift.NewTBinaryProtocolFactoryConf(nil)

	cfg := &thrift.TConfiguration{
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}

	var transportFactory thrift.TTransportFactory = thrift.NewTTransportFactory()

	if *isServer {
		if err := server.RunServer(transportFactory, protocolFactory, *addr, *secure); err != nil {
			fmt.Println("error running server:", err)
		}
	} else {
		if err := client.RunClient(transportFactory, protocolFactory, *addr, *secure, cfg); err != nil {
			fmt.Println("error running client:", err)
		}
	}
}
