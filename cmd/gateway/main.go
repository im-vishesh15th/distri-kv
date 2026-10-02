// Command gateway is the public HTTP front door: API-key auth, per-tenant
// rate limits and key namespacing in front of a DistriKV cluster.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "modernc.org/sqlite" // pure-Go SQLite driver (keeps CGO_ENABLED=0 builds working)

	"distrikv/internal/gateway"
	"distrikv/internal/shard"
)

func main() {
	var (
		dbPath    = flag.String("db", "gateway.db", "SQLite file holding tenants and hashed API keys")
		kvAddrs   = flag.String("kv-addrs", "127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083", "comma-separated cluster node addresses")
		shardPath = flag.String("shard-config", "", "optional JSON file with shard map (for multi-group clusters)")
		bind      = flag.String("bind", ":9090", "HTTP listen address")
		rps       = flag.Float64("rate-limit-rps", 1000, "default per-tenant requests/second")
		burst     = flag.Int("rate-limit-burst", 2000, "default per-tenant burst")
		poolSize  = flag.Int("pool-size", 8, "number of cluster clients (each has its own session)")
		maxBody   = flag.Int64("max-body-bytes", 1<<20, "maximum request body size")
		maxValue  = flag.Int("max-value-bytes", 256<<10, "maximum value size")
		timeout   = flag.Duration("request-timeout", 5*time.Second, "per-request deadline toward the cluster")
		maxConc   = flag.Int("max-concurrent-per-tenant", 128, "max in-flight requests per tenant (extra requests get 429)")
		tlsCert   = flag.String("tls-cert", "", "TLS certificate file (enables HTTPS; needs -tls-key)")
		tlsKey    = flag.String("tls-key", "", "TLS private key file (needs -tls-cert)")
		metrics   = flag.String("metrics-addr", "", "serve Prometheus /metrics on this private address (e.g. :9100); empty disables")

		ctlBind    = flag.String("control-bind", "", "serve the web-console control API on this address (e.g. :9091); empty disables")
		ctlOrigins = flag.String("control-origins", "", "comma-separated browser origins allowed to call the control API (e.g. https://console.example.com)")
		ctlSignup  = flag.Bool("control-signup", true, "allow self-service signup through the control API")
		ctlSecure  = flag.Bool("control-cookie-secure", false, "mark the session cookie Secure (set when the console is served over HTTPS)")
		ctlXFF     = flag.Bool("control-trust-xff", false, "use the last X-Forwarded-For hop as the client IP (only behind your own proxy)")
		ctlTTL     = flag.Duration("control-session-ttl", 24*time.Hour, "console login session lifetime")
	)
	flag.Parse()

	store, err := gateway.OpenSQLite(*dbPath)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer store.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	addrs := strings.Split(*kvAddrs, ",")
	for i := range addrs {
		addrs[i] = strings.TrimSpace(addrs[i])
	}

	var shardCfg *shard.Config
	if *shardPath != "" {
		cfg, err := shard.LoadConfig(*shardPath)
		if err != nil {
			log.Fatalf("load shard config: %v", err)
		}
		shardCfg = cfg
		log.Printf("loaded shard config: %d groups", cfg.Groups)
	}

	pool, err := gateway.NewPool(ctx, addrs, *poolSize, shardCfg)
	if err != nil {
		log.Fatalf("connect to cluster: %v", err)
	}

	gw := gateway.New(gateway.Config{
		BindAddr:       *bind,
		RateLimitRPS:   *rps,
		RateLimitBurst: *burst,
		MaxBodyBytes:   *maxBody,
		MaxValueBytes:  *maxValue,
		RequestTimeout: *timeout,

		MaxConcurrentPerTenant: *maxConc,
		TLSCertFile:            *tlsCert,
		TLSKeyFile:             *tlsKey,
		MetricsAddr:            *metrics,
	}, store, pool)
	defer gw.Close()

	if *ctlBind != "" {
		var origins []string
		for _, o := range strings.Split(*ctlOrigins, ",") {
			if o = strings.TrimSpace(o); o != "" {
				origins = append(origins, o)
			}
		}
		api, err := gateway.NewControlAPI(gateway.ControlConfig{
			BindAddr:          *ctlBind,
			AllowedOrigins:    origins,
			AllowSignup:       *ctlSignup,
			CookieSecure:      *ctlSecure || *tlsCert != "",
			SessionTTL:        *ctlTTL,
			TLSCertFile:       *tlsCert,
			TLSKeyFile:        *tlsKey,
			TrustForwardedFor: *ctlXFF,
			DefaultRateRPS:    *rps,
			DefaultRateBurst:  *burst,
			MaxConcurrent:     *maxConc,
			Usage:             gw.Metrics().Usage,
			UsageSeries:       gw.Metrics().Series,
			Logger:            slog.New(slog.NewJSONHandler(os.Stderr, nil)),
		}, store)
		if err != nil {
			log.Fatalf("control api: %v", err)
		}
		go func() {
			log.Printf("control API listening on %s (signup=%v, origins=%v)", *ctlBind, *ctlSignup, origins)
			if err := api.Run(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("control api: %v", err)
				stop() // a dead control plane should not leave a half-running process
			}
		}()
	}

	if err := gw.Run(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("gateway: %v", err)
	}
}
