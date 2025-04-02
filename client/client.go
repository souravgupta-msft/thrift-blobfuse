package client

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"thrift-blobfuse/gen-go/dcache"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
)

func computeMD5Hash(data []byte) string {
	hash := md5.Sum(data)
	return hex.EncodeToString(hash[:])
}

func getStripe(ctx context.Context, client *dcache.StripeServiceClient, stripeID string) (err error) {
	stripe, err := client.GetStripe(ctx, stripeID)
	if err != nil {
		fmt.Printf("error getting stripe %v : %v\n", stripeID, err)
	} else {
		fmt.Printf("Got Stripe %v, ID: %s, Offset: %d, Length: %d, Hash: %s, Data length: %v\n", stripeID, stripe.ID, stripe.Offset, stripe.Length, stripe.Hash, len(stripe.Data))
		fmt.Printf("Stripe Hash: %v\n", computeMD5Hash(stripe.Data))
	}

	return nil
}

func handleClient(client *dcache.StripeServiceClient) (err error) {
	var defaultCtx = context.Background()
	var stripeSize int64 = 16 * 1024 * 1024 //16MB

	err = client.Ping(defaultCtx)
	if err != nil {
		fmt.Println("error pinging server:", err)
		return err
	}

	for i := 1; i < 5; i++ {
		// go func(i int) {
		err = getStripe(defaultCtx, client, fmt.Sprintf("stripeID%d-0-%d", i, stripeSize))
		if err != nil {
			fmt.Printf("error getting stripe %v : %v\n", i, err)
		} else {
			fmt.Printf("Stripe get successfully %v\n", i)
		}
		// }(i)
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

	time.Sleep(2 * time.Second)

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
