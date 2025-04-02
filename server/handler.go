package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"thrift-blobfuse/gen-go/dcache"
)

// type check to ensure that StripeServiceHandler implements dcache.StripeService interface
var _ dcache.StripeService = &StripeServiceHandler{}

type StripeServiceHandler struct {
	cacheDir string
}

func NewStripeServiceHandler() *StripeServiceHandler {
	return &StripeServiceHandler{
		cacheDir: "/home/sourav/dcache",
	}
}

func (h *StripeServiceHandler) Ping(ctx context.Context) error {
	fmt.Println("Ping called")
	return nil
}

func (h *StripeServiceHandler) GetStripe(ctx context.Context, stripeID string) (*dcache.Stripe, error) {
	fmt.Printf("GetStripe called for stripe ID %v\n", stripeID)

	stripeFilePath := filepath.Join(h.cacheDir, stripeID)
	data, err := os.ReadFile(stripeFilePath)
	if err != nil {
		fmt.Printf("Error reading stripe file [%v]\n", err.Error())
		return nil, err
	}

	return &dcache.Stripe{
		ID: stripeID,
		// Offset: offset,
		// Length: stripeLength,
		Hash: "stripeHash",
		Data: data,
	}, nil
}

func (h *StripeServiceHandler) PutStripe(ctx context.Context, stripe *dcache.Stripe) error {
	// should be written once, take locks
	fmt.Printf("PutStripe called for stripe ID %v, offset %v, stripe length %v, hash %v, data length %v\n",
		stripe.ID, stripe.Offset, stripe.Length, stripe.Hash, len(stripe.Data))

	stripeFilePath := filepath.Join(h.cacheDir, fmt.Sprintf("%s-%d-%d", stripe.ID, stripe.Offset, stripe.Length))
	err := os.WriteFile(stripeFilePath, stripe.Data, 0400)
	if err != nil {
		fmt.Printf("Error writing stripe file [%v]\n", err.Error())
		return err
	}

	fmt.Printf("PutStripe: Stripe written successfully, hash: %v\n", stripe.Hash)

	return nil
}

func (h *StripeServiceHandler) RemoveStripe(ctx context.Context, stripeID string) error {
	fmt.Printf("RemoveStripe called for stripe ID %v\n", stripeID)

	stripeFilePath := filepath.Join(h.cacheDir, stripeID)
	err := os.Remove(stripeFilePath)
	if err != nil {
		fmt.Printf("Error removing stripe file [%v]\n", err.Error())
		return err
	}

	return nil
}
