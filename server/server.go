package server

import (
	"crypto/tls"
	"fmt"
	"thrift-blobfuse/gen-go/dcache"

	"github.com/apache/thrift/lib/go/thrift"
)

func RunServer(transportFactory thrift.TTransportFactory, protocolFactory thrift.TProtocolFactory, addr string, secure bool) error {
	var transport thrift.TServerTransport
	var err error
	if secure {
		cfg := new(tls.Config)
		if cert, err := tls.LoadX509KeyPair("server.crt", "server.key"); err == nil {
			cfg.Certificates = append(cfg.Certificates, cert)
		} else {
			return err
		}
		transport, err = thrift.NewTSSLServerSocket(addr, cfg)
	} else {
		transport, err = thrift.NewTServerSocket(addr)
	}

	if err != nil {
		return err
	}
	fmt.Printf("%T\n", transport)
	handler := NewStripeServiceHandler()
	processor := dcache.NewStripeServiceProcessor(handler)
	server := thrift.NewTSimpleServer4(processor, transport, transportFactory, protocolFactory)

	// go func() {
	// 	time.Sleep(10 * time.Second)
	// 	err = server.Stop()
	// 	if err != nil {
	// 		fmt.Println("Error stopping server:", err)
	// 	}
	// 	fmt.Println("Server stopped")
	// }()

	fmt.Println("Starting the simple server... on ", addr)
	err = server.Serve()
	if err != nil {
		fmt.Println("Error starting server:", err)
	}

	return err
}
