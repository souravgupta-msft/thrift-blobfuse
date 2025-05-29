package client

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"thrift-blobfuse/gen-go/dcache"

	"github.com/apache/thrift/lib/go/thrift"
)

func computeMD5Hash(data []byte) string {
	hash := md5.Sum(data)
	return hex.EncodeToString(hash[:])
}

func getStripe(ctx context.Context, client *dcache.StripeServiceClient, req *dcache.GetStripeRequest) (err error) {
	fmt.Printf("Before sending RPC, request data Hash: %v\n", computeMD5Hash(req.Data))

	resp, err := client.GetStripe(ctx, req)
	if err != nil {
		fmt.Printf("error getting stripe %v : %v\n", req.StripeID, err)
		return err
	}

	fmt.Printf("Got Stripe %v, Hash: %v, Data length: %v, bytes read: %v\n",
		req.StripeID, resp.Hash, len(req.Data), resp.BytesRead)
	fmt.Printf("Request data Hash: %v\n", computeMD5Hash(req.Data))
	fmt.Printf("Response data Hash: %v\n", computeMD5Hash(resp.Data))
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

	data := make([]byte, stripeSize)
	getStripeReq := &dcache.GetStripeRequest{
		StripeID: fmt.Sprintf("stripeID1-0-%d", stripeSize),
		Data:     data,
	}

	fmt.Printf("Original data Hash: %v\n", computeMD5Hash(data))
	err = getStripe(defaultCtx, client, getStripeReq)
	if err != nil {
		fmt.Printf("error getting stripe %v : %v\n", getStripeReq.StripeID, err)
	}

	/*
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
	*/

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
