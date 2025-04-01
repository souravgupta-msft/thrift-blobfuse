package client

import (
	"context"
	"fmt"
	"thrift-blobfuse/gen-go/dcache"

	"github.com/apache/thrift/lib/go/thrift"
)

func handleClient(client *dcache.StripeServiceClient) (err error) {
	var defaultCtx = context.Background()
	var stripeSize int64 = 16 * 1024 * 1024 //16MB

	// get stripe of 16MB
	stripe, err := client.GetStripe(defaultCtx, fmt.Sprintf("stripeID1-0-%d", stripeSize))
	if err != nil {
		fmt.Println("error getting stripe:", err)
	} else {
		fmt.Printf("Got Stripe, ID: %s, Offset: %d, Length: %d, Hash: %s, Data length: %v\n", stripe.ID, stripe.Offset, stripe.Length, stripe.Hash, len(stripe.Data))
	}

	// go func() {
	// put stripe
	err = client.PutStripe(defaultCtx, &dcache.Stripe{
		ID:     "stripeID1",
		Offset: stripeSize,
		Length: stripeSize,
		Hash:   "stripeHash1",
		Data:   make([]byte, stripeSize),
	})
	if err != nil {
		fmt.Println("error putting stripe 1:", err)
	} else {
		fmt.Println("Stripe put successfully 1")
	}
	// }()

	// go func() {
	err = client.PutStripe(defaultCtx, &dcache.Stripe{
		ID:     "stripeID1",
		Offset: stripeSize,
		Length: stripeSize,
		Hash:   "stripeHash2",
		Data:   make([]byte, stripeSize),
	})
	if err != nil {
		fmt.Println("error putting stripe 2:", err)
	} else {
		fmt.Println("Stripe put successfully 2")
	}
	// }()

	// time.Sleep(2 * time.Second)

	// remove stripe
	err = client.RemoveStripe(defaultCtx, fmt.Sprintf("stripeID1-%d-%d", stripeSize, stripeSize))
	if err != nil {
		fmt.Println("error removing stripe:", err)
	} else {
		fmt.Println("Stripe removed successfully")
	}

	return nil
}

func RunClient(transportFactory thrift.TTransportFactory, protocolFactory thrift.TProtocolFactory, addr string, secure bool, cfg *thrift.TConfiguration) error {
	var transport thrift.TTransport
	if secure {
		transport = thrift.NewTSSLSocketConf(addr, cfg)
	} else {
		transport = thrift.NewTSocketConf(addr, cfg)
	}
	transport, err := transportFactory.GetTransport(transport)
	if err != nil {
		return err
	}
	defer transport.Close()
	if err := transport.Open(); err != nil {
		return err
	}
	iprot := protocolFactory.GetProtocol(transport)
	oprot := protocolFactory.GetProtocol(transport)
	return handleClient(dcache.NewStripeServiceClient(thrift.NewTStandardClient(iprot, oprot)))
}
