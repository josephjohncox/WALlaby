//go:build ignore

package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
)

type event struct {
	Action string `json:"Action"`
	Test   string `json:"Test"`
}

func main() {
	results := flag.String("results", "", "go test -json output")
	requiredRaw := flag.String("required", "", "comma-separated required test names")
	flag.Parse()
	if *results == "" || strings.TrimSpace(*requiredRaw) == "" {
		fmt.Fprintln(os.Stderr, "results and required tests are required")
		os.Exit(2)
	}
	required := map[string]bool{}
	for _, name := range strings.Split(*requiredRaw, ",") {
		name = strings.TrimSpace(name)
		if name != "" {
			required[name] = false
		}
	}
	file, err := os.Open(*results)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	defer file.Close()
	skipped := map[string]bool{}
	failed := map[string]bool{}
	scanner := bufio.NewScanner(file)
	buffer := make([]byte, 64*1024)
	scanner.Buffer(buffer, 4*1024*1024)
	for scanner.Scan() {
		var item event
		if json.Unmarshal(scanner.Bytes(), &item) != nil || item.Test == "" {
			continue
		}
		if _, ok := required[item.Test]; !ok {
			continue
		}
		switch item.Action {
		case "pass":
			required[item.Test] = true
		case "skip":
			skipped[item.Test] = true
		case "fail":
			failed[item.Test] = true
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	var missing []string
	for name, passed := range required {
		if !passed {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 || len(skipped) > 0 || len(failed) > 0 {
		fmt.Fprintf(os.Stderr, "durability evidence incomplete: missing=%v skipped=%v failed=%v\n", missing, sortedKeys(skipped), sortedKeys(failed))
		os.Exit(1)
	}
	fmt.Printf("verified %d required tests: present, passed, and not skipped\n", len(required))
}

func sortedKeys(values map[string]bool) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
