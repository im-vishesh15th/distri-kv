// Command gatewayctl manages tenants and API keys in the gateway's SQLite
// database. Run it on the same machine as the gateway.
//
//	gatewayctl -db gateway.db create-tenant shoestore "Shoe Store"
//	gatewayctl -db gateway.db create-key shoestore prod [720h]
//	gatewayctl -db gateway.db list-keys shoestore
//	gatewayctl -db gateway.db revoke-key shoestore dkv_live_ab12cd34
//	gatewayctl -db gateway.db rotate-key shoestore dkv_live_ab12cd34 [720h]
//	gatewayctl -db gateway.db set-quota shoestore 200 400
//	gatewayctl -db gateway.db create-admin ops@example.com      (password from $DKV_PASSWORD or stdin)
//	gatewayctl -db gateway.db reset-password user@example.com   (password from $DKV_PASSWORD or stdin)
//	gatewayctl -db gateway.db backup /backups/gateway-2026-10-02.db
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	"distrikv/internal/gateway"
)

func usage() {
	fmt.Fprint(os.Stderr, `usage: gatewayctl [-db gateway.db] <command> [args]

commands:
  create-tenant <id> [display name]     id: lowercase letters, digits, '-' or '_'
  create-key    <tenant> <name> [ttl]   prints the key ONCE; ttl like 720h (default: never expires)
  list-keys     <tenant>                shows key prefixes only
  revoke-key    <tenant> <prefix>       takes effect immediately
  rotate-key    <tenant> <prefix> [ttl] creates a replacement, revokes the old key
  set-quota     <tenant> <rps> <burst>  per-tenant rate limit (0 0 = gateway default)
  create-admin  <email>                 operator account for the console admin API;
                                        password from $DKV_PASSWORD or one line on stdin
  reset-password <email>                new password (same input rules); signs the user out everywhere
  backup        <path>                  consistent copy of the whole database (path must not exist)
`)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}

func need(args []string, n int) {
	if len(args) < n {
		usage()
		os.Exit(2)
	}
}

// readPassword takes the password from $DKV_PASSWORD, else one line of stdin,
// so it never appears in the process list or shell history.
func readPassword() string {
	if p := os.Getenv("DKV_PASSWORD"); p != "" {
		return p
	}
	line, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil && line == "" {
		fatal(fmt.Errorf("no password: set DKV_PASSWORD or pipe it on stdin"))
	}
	return strings.TrimRight(line, "\r\n")
}

func parseTTL(s string) time.Duration {
	if s == "" {
		return 0
	}
	d, err := time.ParseDuration(s)
	if err != nil || d < 0 {
		fatal(fmt.Errorf("bad ttl %q (examples: 24h, 720h)", s))
	}
	return d
}

func main() {
	dbPath := flag.String("db", "gateway.db", "SQLite database file")
	flag.Usage = usage
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}

	store, err := gateway.OpenSQLite(*dbPath)
	if err != nil {
		fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	now := time.Now()

	switch args[0] {
	case "create-tenant":
		need(args, 2)
		name := strings.Join(args[2:], " ")
		t, err := gateway.CreateTenant(ctx, store, args[1], name, now)
		if err != nil {
			fatal(err)
		}
		fmt.Printf("created tenant %s (%s)\n", t.ID, t.Name)

	case "create-key":
		need(args, 3)
		ttl := ""
		if len(args) > 3 {
			ttl = args[3]
		}
		key, rec, err := gateway.CreateAPIKey(ctx, store, args[1], args[2], parseTTL(ttl), now)
		if err != nil {
			fatal(err)
		}
		fmt.Printf("API key for %s (%s). Copy it now, it cannot be shown again:\n\n  %s\n\nprefix: %s\n",
			args[1], rec.Name, key, rec.Prefix)

	case "list-keys":
		need(args, 2)
		keys, err := store.ListKeys(ctx, args[1])
		if err != nil {
			fatal(err)
		}
		if len(keys) == 0 {
			fmt.Println("no keys")
		}
		for _, k := range keys {
			state := "active"
			switch {
			case k.RevokedAt != nil:
				state = "revoked"
			case k.ExpiresAt != nil && !now.Before(*k.ExpiresAt):
				state = "expired"
			}
			exp := "never"
			if k.ExpiresAt != nil {
				exp = k.ExpiresAt.Format(time.RFC3339)
			}
			fmt.Printf("%s  %-8s  name=%s  created=%s  expires=%s\n",
				k.Prefix, state, k.Name, k.CreatedAt.Format(time.RFC3339), exp)
		}

	case "revoke-key":
		need(args, 3)
		if err := store.RevokeKey(ctx, args[1], args[2], now); err != nil {
			fatal(err)
		}
		fmt.Println("revoked", args[2])

	case "rotate-key":
		need(args, 3)
		ttl := ""
		if len(args) > 3 {
			ttl = args[3]
		}
		key, rec, err := gateway.RotateAPIKey(ctx, store, args[1], args[2], parseTTL(ttl), now)
		if err != nil {
			fatal(err)
		}
		fmt.Printf("revoked %s\nnew key (shown once):\n\n  %s\n\nprefix: %s\n", args[2], key, rec.Prefix)

	case "set-quota":
		need(args, 4)
		rps, err1 := strconv.ParseFloat(args[2], 64)
		burst, err2 := strconv.Atoi(args[3])
		if err1 != nil || err2 != nil || rps < 0 || burst < 0 {
			fatal(fmt.Errorf("rps and burst must be non-negative numbers"))
		}
		if err := store.SetTenantQuota(ctx, args[1], rps, burst); err != nil {
			fatal(err)
		}
		fmt.Printf("quota for %s: %g req/s, burst %d\n", args[1], rps, burst)

	case "create-admin":
		need(args, 2)
		u, err := gateway.CreateAdminUser(ctx, store, args[1], readPassword(), 0, now)
		if err != nil {
			fatal(err)
		}
		fmt.Printf("created operator %s (id %s)\n", u.Email, u.ID)

	case "reset-password":
		need(args, 2)
		if err := gateway.ResetUserPassword(ctx, store, args[1], readPassword(), 0); err != nil {
			fatal(err)
		}
		fmt.Println("password updated; all sessions for this user were ended")

	case "backup":
		need(args, 2)
		if err := store.Backup(ctx, args[1]); err != nil {
			fatal(err)
		}
		fmt.Println("backup written to", args[1])

	default:
		usage()
		os.Exit(2)
	}
}
