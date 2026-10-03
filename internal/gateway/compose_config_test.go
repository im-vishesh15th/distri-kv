package gateway

// Regression guard for the compose files. A compose override that defines its own
// `command:` REPLACES the base command, so any flag added to the base gateway
// (e.g. -node-metrics-urls, which turns on tenant storage accounting) silently
// disappears when the override does not repeat it. These tests parse the files as
// text (no YAML dependency) and fail if that happens again.

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

var composeFiles = []string{
	"../../docker-compose.yml",
	"../../docker-compose.multigroup.yml",
	"../../deployments/docker-compose.prod.yml",
}

const wantNodeMetrics = "-node-metrics-urls=http://distrikv-1:9101,http://distrikv-2:9101,http://distrikv-3:9101"

var (
	svcRe  = regexp.MustCompile(`^  ([A-Za-z0-9_-]+):\s*$`)
	flagRe = regexp.MustCompile(`(?:^|\s)(-[a-z][a-z0-9-]*)(?:=|\s|$)`)
)

// serviceBlocks returns the raw text of each service under the top-level `services:`.
func serviceBlocks(t *testing.T, path string) map[string]string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	inServices, cur := false, ""
	for _, line := range strings.Split(string(raw), "\n") {
		switch {
		case strings.HasPrefix(line, "services:"):
			inServices = true
		case inServices && line != "" && !strings.HasPrefix(line, " ") && !strings.HasPrefix(line, "#"):
			inServices, cur = false, "" // next top-level key (networks:, volumes:, ...)
		case inServices:
			if m := svcRe.FindStringSubmatch(line); m != nil {
				cur = m[1]
				continue
			}
			if cur != "" {
				out[cur] += line + "\n"
			}
		}
	}
	return out
}

// commandFlags returns the set of flag names in a service's `command:` block.
func commandFlags(block string) map[string]bool {
	i := strings.Index(block, "command:")
	if i < 0 {
		return nil
	}
	rest := block[i+len("command:"):]
	var cmd []string
	for n, line := range strings.Split(rest, "\n") {
		if n > 0 && regexp.MustCompile(`^    [a-z_]+:`).MatchString(line) {
			break // next key of the service
		}
		cmd = append(cmd, line)
	}
	flags := map[string]bool{}
	for _, m := range flagRe.FindAllStringSubmatch(strings.Join(cmd, "\n"), -1) {
		flags[m[1]] = true
	}
	return flags
}

func TestComposeGatewayEnablesStorageSource(t *testing.T) {
	for _, f := range composeFiles {
		gw := serviceBlocks(t, f)["gateway"]
		if gw == "" {
			t.Fatalf("%s: no gateway service", f)
		}
		if !strings.Contains(gw, wantNodeMetrics) {
			t.Errorf("%s: gateway command lacks %s (tenant storage would be disabled)", f, wantNodeMetrics)
		}
	}
}

// Every flag of the base gateway/node command must survive in the multigroup
// override, and nothing the override already had may be dropped.
func TestComposeMultigroupKeepsBaseFlags(t *testing.T) {
	base := serviceBlocks(t, "../../docker-compose.yml")
	over := serviceBlocks(t, "../../docker-compose.multigroup.yml")
	for _, svc := range []string{"gateway", "distrikv-1", "distrikv-2", "distrikv-3"} {
		bf, of := commandFlags(base[svc]), commandFlags(over[svc])
		if of == nil {
			t.Fatalf("multigroup: %s has no command", svc)
		}
		for f := range bf {
			if !of[f] {
				t.Errorf("multigroup %s command is missing base flag %s", svc, f)
			}
		}
		if !of["-shard-config"] {
			t.Errorf("multigroup %s lost -shard-config", svc)
		}
		if !strings.Contains(over[svc], "shard.json:/etc/distrikv/shard.json:ro") {
			t.Errorf("multigroup %s lost the shard.json volume mount", svc)
		}
	}
	for _, f := range []string{"-db", "-bind", "-metrics-addr", "-control-bind", "-control-origins", "-kv-addrs", "-shard-config", "-node-metrics-urls"} {
		if !commandFlags(over["gateway"])[f] {
			t.Errorf("multigroup gateway lacks %s", f)
		}
	}
}

// The gateway polls <node>:9101, so every node must serve metrics there.
func TestComposeNodesServeMetricsOn9101(t *testing.T) {
	for _, f := range composeFiles {
		blocks := serviceBlocks(t, f)
		for _, n := range []string{"distrikv-1", "distrikv-2", "distrikv-3"} {
			if !strings.Contains(blocks[n], "-metrics-addr=:9101") {
				t.Errorf("%s: %s does not set -metrics-addr=:9101", f, n)
			}
		}
	}
}
