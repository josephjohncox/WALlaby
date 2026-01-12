package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

type benchResult struct {
	Name        string  `json:"name"`
	Procs       int     `json:"procs,omitempty"`
	Iterations  int64   `json:"iterations"`
	NsPerOp     float64 `json:"ns_per_op"`
	MBPerSecond float64 `json:"mb_per_s,omitempty"`
	BytesPerOp  float64 `json:"bytes_per_op,omitempty"`
	AllocsPerOp float64 `json:"allocs_per_op,omitempty"`
}

func main() {
	inputPath := flag.String("input", "-", "benchmark output file (default: stdin)")
	format := flag.String("format", "json", "output format: json or csv")
	pretty := flag.Bool("pretty", true, "pretty-print JSON output")
	flag.Parse()

	reader, err := openInput(*inputPath)
	if err != nil {
		fatal(err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close input: %v\n", err)
		}
	}()

	results, err := parseBench(reader)
	if err != nil {
		fatal(err)
	}

	switch strings.ToLower(strings.TrimSpace(*format)) {
	case "json":
		if err := writeJSON(os.Stdout, results, *pretty); err != nil {
			fatal(err)
		}
	case "csv":
		if err := writeCSV(os.Stdout, results); err != nil {
			fatal(err)
		}
	default:
		fatal(fmt.Errorf("unsupported format %q", *format))
	}
}

type readCloser struct {
	io.Reader
	closeFn func() error
}

func (r readCloser) Close() error {
	if r.closeFn != nil {
		return r.closeFn()
	}
	return nil
}

func openInput(path string) (io.ReadCloser, error) {
	if path == "" || path == "-" {
		return readCloser{Reader: os.Stdin}, nil
	}
	// #nosec G304 -- input path provided via CLI flag.
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func parseBench(r io.Reader) ([]benchResult, error) {
	scanner := bufio.NewScanner(r)
	results := make([]benchResult, 0, 64)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if !strings.HasPrefix(line, "Benchmark") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		result, ok := parseBenchFields(fields)
		if ok {
			results = append(results, result)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

func parseBenchFields(fields []string) (benchResult, bool) {
	iters, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return benchResult{}, false
	}

	nsPerOp, err := parseDuration(fields[2], fields[3])
	if err != nil {
		return benchResult{}, false
	}

	name, procs := splitNameProcs(fields[0])
	result := benchResult{
		Name:       name,
		Procs:      procs,
		Iterations: iters,
		NsPerOp:    nsPerOp,
	}

	for i := 4; i+1 < len(fields); i += 2 {
		value := fields[i]
		unit := fields[i+1]
		switch unit {
		case "MB/s":
			if parsed, err := strconv.ParseFloat(value, 64); err == nil {
				result.MBPerSecond = parsed
			}
		case "B/op":
			if parsed, err := strconv.ParseFloat(value, 64); err == nil {
				result.BytesPerOp = parsed
			}
		case "allocs/op":
			if parsed, err := strconv.ParseFloat(value, 64); err == nil {
				result.AllocsPerOp = parsed
			}
		}
	}

	return result, true
}

func parseDuration(value, unit string) (float64, error) {
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, err
	}

	switch unit {
	case "ns/op":
		return parsed, nil
	case "us/op", "µs/op":
		return parsed * 1e3, nil
	case "ms/op":
		return parsed * 1e6, nil
	case "s/op":
		return parsed * 1e9, nil
	default:
		return 0, fmt.Errorf("unknown duration unit %q", unit)
	}
}

func splitNameProcs(name string) (string, int) {
	idx := strings.LastIndex(name, "-")
	if idx <= 0 || idx >= len(name)-1 {
		return name, 0
	}
	procs, err := strconv.Atoi(name[idx+1:])
	if err != nil {
		return name, 0
	}
	return name[:idx], procs
}

func writeJSON(w io.Writer, results []benchResult, pretty bool) error {
	enc := json.NewEncoder(w)
	if pretty {
		enc.SetIndent("", "  ")
	}
	return enc.Encode(results)
}

func writeCSV(w io.Writer, results []benchResult) error {
	writer := csv.NewWriter(w)
	if err := writer.Write([]string{"name", "procs", "iterations", "ns_per_op", "mb_per_s", "bytes_per_op", "allocs_per_op"}); err != nil {
		return err
	}
	for _, result := range results {
		row := []string{
			result.Name,
			strconv.Itoa(result.Procs),
			strconv.FormatInt(result.Iterations, 10),
			formatFloat(result.NsPerOp),
			formatFloat(result.MBPerSecond),
			formatFloat(result.BytesPerOp),
			formatFloat(result.AllocsPerOp),
		}
		if err := writer.Write(row); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func formatFloat(value float64) string {
	if value == 0 {
		return ""
	}
	return strconv.FormatFloat(value, 'f', -1, 64)
}

func fatal(err error) {
	if err == nil {
		return
	}
	if errors.Is(err, flag.ErrHelp) {
		os.Exit(0)
	}
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
