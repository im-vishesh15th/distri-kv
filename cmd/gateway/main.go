package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"distrikv/internal/gateway"
)

func main() {
	var (
		kvAddrs      = flag.String("kv-addrs", "127.0.0.1:8081", "comma-separated list of KV server addresses")
		bindAddr     = flag.String("bind", ":9090", "HTTP gateway bind address")
		rateLimitRPS = flag.Float64("rate-limit-rps", 1000, "global rate limit requests per second per tenant")
		rateBurst    = flag.Int("rate-limit-burst", 2000, "rate limit burst allowance per tenant")
	)
	flag.Parse()

	addrs := strings.Split(*kvAddrs, ",")

	cfg := gateway.Config{
		KVAddresses:   addrs,
		BindAddr:      *bindAddr,
		RateLimitRPS:  *rateLimitRPS,
		RateLimitBurst: *rateBurst,
	}

	gw, err := gateway.NewGateway(cfg)
	if err != nil {
		log.Fatalf("failed to create gateway: %v", err)
	}
	defer gw.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := gw.Run(ctx); err != nil && err != http.ErrServerClosed {
		log.Fatalf("gateway run: %v", err)
	}
}