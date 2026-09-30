// Command client is a CLI for manual smoke testing of the DistriKV API.
//
// Usage:
//
//	client [flags] <command> [args]
//
// Commands:
//
//	get <key>                print the value
//	set <key> <value>        store value (upsert)
//	del <key>                delete key
//	exists <key>             print true/false
//	cas <key> <expected|-> <new>   compare-and-swap ("-" = key must be absent)
//	incr <key> [delta]       add delta (default 1) to int64 value
//	decr <key>               subtract 1 from int64 value
//	status                   print this node's routing view (role/leader)
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"distrikv/pkg/client"
)

func main() {
	addr := flag.String("addr", "localhost:8080", "DistriKV server address")
	timeout := flag.Duration("timeout", 5*time.Second, "request timeout")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	c, err := client.Dial(ctx, *addr)
	if err != nil {
		fatal(err)
	}
	defer c.Close()

	if err := dispatch(ctx, c, args); err != nil {
		fatal(err)
	}
}

func dispatch(ctx context.Context, c *client.Client, args []string) error {
	cmd, rest := args[0], args[1:]
	need := func(n int) error {
		if len(rest) != n {
			return fmt.Errorf("%s: want %d arg(s), got %d", cmd, n, len(rest))
		}
		return nil
	}

	switch cmd {
	case "get":
		if err := need(1); err != nil {
			return err
		}
		v, err := c.Get(ctx, rest[0])
		if err != nil {
			return err
		}
		fmt.Println(string(v))

	case "set":
		if err := need(2); err != nil {
			return err
		}
		return c.Put(ctx, rest[0], []byte(rest[1]))

	case "del":
		if err := need(1); err != nil {
			return err
		}
		return c.Delete(ctx, rest[0])

	case "exists":
		if err := need(1); err != nil {
			return err
		}
		ok, err := c.Exists(ctx, rest[0])
		if err != nil {
			return err
		}
		fmt.Println(strconv.FormatBool(ok))

	case "cas":
		if err := need(3); err != nil {
			return err
		}
		var expected []byte
		if rest[1] != "-" { // "-" = key must be absent
			expected = []byte(rest[1])
		}
		applied, err := c.CAS(ctx, rest[0], expected, []byte(rest[2]))
		if err != nil {
			return err
		}
		fmt.Println(strconv.FormatBool(applied))

	case "incr":
		var delta int64 = 1
		switch len(rest) {
		case 1:
		case 2:
			d, err := strconv.ParseInt(rest[1], 10, 64)
			if err != nil {
				return fmt.Errorf("incr: delta must be an integer: %w", err)
			}
			delta = d
		default:
			return fmt.Errorf("incr: want 1 or 2 args, got %d", len(rest))
		}
		v, err := c.Incr(ctx, rest[0], delta)
		if err != nil {
			return err
		}
		fmt.Println(v)

	case "decr":
		if err := need(1); err != nil {
			return err
		}
		v, err := c.Decr(ctx, rest[0])
		if err != nil {
			return err
		}
		fmt.Println(v)

	case "status":
		if err := need(0); err != nil {
			return err
		}
		st, err := c.Status(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("node=%s role=%s leader=%s", st.NodeID, st.Role, orDash(st.LeaderID))
		if st.LeaderAddr != "" {
			fmt.Printf(" leader_addr=%s", st.LeaderAddr)
		}
		fmt.Println()

	default:
		usage()
		return fmt.Errorf("unknown command %q", cmd)
	}
	return nil
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: client [flags] <command> [args]

commands:
  get <key>
  set <key> <value>
  del <key>
  exists <key>
  cas <key> <expected|-> <new>    ("-" = key must be absent)
  incr <key> [delta]
  decr <key>
  status`)
	flag.PrintDefaults()
}

// orDash renders an empty ID readably.
func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
