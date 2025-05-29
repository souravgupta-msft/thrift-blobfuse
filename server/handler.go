package server

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"thrift-blobfuse/gen-go/dcache"
)

// type check to ensure that StripeServiceHandler implements dcache.StripeService interface
var _ dcache.StripeService = &StripeServiceHandler{}

type StripeServiceHandler struct {
	mu       sync.Mutex
	cacheDir string
}

func NewStripeServiceHandler() *StripeServiceHandler {
	return &StripeServiceHandler{
		cacheDir: "/home/sourav/dcache",
	}
}

func computeMD5Hash(data []byte) string {
	hash := md5.Sum(data)
	return hex.EncodeToString(hash[:])
}

func (h *StripeServiceHandler) Ping(ctx context.Context) error {
	fmt.Println("Ping called")
	return nil
}

func (h *StripeServiceHandler) GetStripe(ctx context.Context, req *dcache.GetStripeRequest) (*dcache.GetStripeResponse, error) {
	fmt.Printf("GetStripe called for stripe ID %v\n", req.StripeID)

	stripeFilePath := filepath.Join(h.cacheDir, req.StripeID)

	fh, err := os.Open(stripeFilePath)
	if err != nil {
		fmt.Printf("Error opening stripe file [%v]\n", err.Error())
		return nil, err
	}
	defer fh.Close()

	n, err := fh.Read(req.Data)
	if err != nil {
		fmt.Printf("Error reading stripe file [%v]\n", err.Error())
		return nil, err
	}
	fmt.Printf("Read %d bytes from stripe file\n", n)

	hash := computeMD5Hash(req.Data)
	fmt.Printf("Hash: %v\n", hash)

	return &dcache.GetStripeResponse{
		BytesRead: int64(n),
		Data:      req.Data,
		Hash:      hash,
	}, nil
}

func (h *StripeServiceHandler) PutStripe(ctx context.Context, stripe *dcache.Stripe) error {
	// should be written once, take locks
	h.mu.Lock()
	defer h.mu.Unlock()

	fmt.Printf("PutStripe called for stripe ID %v, offset %v, stripe length %v, hash %v, data length %v\n",
		stripe.ID, stripe.Offset, stripe.Length, stripe.Hash, len(stripe.Data))

	stripeFilePath := filepath.Join(h.cacheDir, fmt.Sprintf("%s-%d-%d", stripe.ID, stripe.Offset, stripe.Length))
	err := os.WriteFile(stripeFilePath, stripe.Data, 0400)
	if err != nil {
		fmt.Printf("Error writing stripe file, hash %v [%v]\n", stripe.Hash, err.Error())
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
