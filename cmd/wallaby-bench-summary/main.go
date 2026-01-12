package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
	"time"
)

type benchResult struct {
	Target        string  `json:"target"`
	Profile       string  `json:"profile"`
	Scenario      string  `json:"scenario"`
	Records       int64   `json:"records"`
	Bytes         int64   `json:"bytes"`
	DurationSec   float64 `json:"duration_sec"`
	RecordsPerSec float64 `json:"records_per_sec"`
	MBPerSec      float64 `json:"mb_per_sec"`
	LatencyP50Ms  float64 `json:"latency_p50_ms"`
	LatencyP95Ms  float64 `json:"latency_p95_ms"`
	LatencyP99Ms  float64 `json:"latency_p99_ms"`
	StartedAt     string  `json:"started_at"`
	EndedAt       string  `json:"ended_at"`
}

type resultKey struct {
	Target   string
	Profile  string
	Scenario string
}

func main() {
	dir := flag.String("dir", "bench/results", "directory containing bench_*.json files")
	format := flag.String("format", "table", "output format: table|markdown|benchstat|json")
	latest := flag.Bool("latest", true, "show latest result per target/profile/scenario")
	output := flag.String("output", "", "optional output file")
	flag.Parse()

	results, err := loadResults(*dir)
	if err != nil {
		fatal(err)
	}
	if *latest {
		results = latestResults(results)
	}
	if len(results) == 0 {
		fatal(errors.New("no benchmark results found"))
	}

	sortResults(results)

	writer, closer, err := outputWriter(*output)
	if err != nil {
		fatal(err)
	}
	if closer != nil {
		defer func() {
			if err := closer(); err != nil {
				log.Printf("close output: %v", err)
			}
		}()
	}

	switch strings.ToLower(*format) {
	case "table":
		if err := writeTable(writer, results); err != nil {
			fatal(err)
		}
	case "markdown":
		if err := writeMarkdown(writer, results); err != nil {
			fatal(err)
		}
	case "benchstat":
		if err := writeBenchstat(writer, results); err != nil {
			fatal(err)
		}
	case "json":
		if err := writeJSON(writer, results); err != nil {
			fatal(err)
		}
	default:
		fatal(fmt.Errorf("unsupported format %q", *format))
	}
}

func loadResults(dir string) ([]benchResult, error) {
	pattern := filepath.Join(dir, "bench_*.json")
	paths, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("find bench results: %w", err)
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("no results in %s", dir)
	}
	results := make([]benchResult, 0)
	for _, path := range paths {
		// #nosec G304 -- path comes from local benchmark directory.
		file, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("open %s: %w", path, err)
		}
		var batch []benchResult
		if err := json.NewDecoder(file).Decode(&batch); err != nil {
			if closeErr := file.Close(); closeErr != nil {
				fmt.Fprintf(os.Stderr, "close %s: %v\n", path, closeErr)
			}
			return nil, fmt.Errorf("decode %s: %w", path, err)
		}
		if err := file.Close(); err != nil {
			return nil, fmt.Errorf("close %s: %w", path, err)
		}
		results = append(results, batch...)
	}
	return results, nil
}

func latestResults(results []benchResult) []benchResult {
	byKey := make(map[resultKey]benchResult)
	for _, result := range results {
		key := resultKey{Target: result.Target, Profile: result.Profile, Scenario: result.Scenario}
		current, ok := byKey[key]
		if !ok || laterResult(result, current) {
			byKey[key] = result
		}
	}
	latest := make([]benchResult, 0, len(byKey))
	for _, result := range byKey {
		latest = append(latest, result)
	}
	return latest
}

func laterResult(a, b benchResult) bool {
	if parseTime(a.EndedAt).After(parseTime(b.EndedAt)) {
		return true
	}
	return parseTime(a.StartedAt).After(parseTime(b.StartedAt))
}

func parseTime(value string) time.Time {
	if value == "" {
		return time.Time{}
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}
	}
	return parsed
}

func sortResults(results []benchResult) {
	profileOrder := map[string]int{"small": 0, "medium": 1, "large": 2}
	scenarioOrder := map[string]int{"base": 0, "ddl": 1}
	sort.Slice(results, func(i, j int) bool {
		a := results[i]
		b := results[j]
		if scenarioOrder[a.Scenario] != scenarioOrder[b.Scenario] {
			return scenarioOrder[a.Scenario] < scenarioOrder[b.Scenario]
		}
		if profileOrder[a.Profile] != profileOrder[b.Profile] {
			return profileOrder[a.Profile] < profileOrder[b.Profile]
		}
		if a.Target != b.Target {
			return a.Target < b.Target
		}
		return a.EndedAt < b.EndedAt
	})
}

func outputWriter(path string) (io.Writer, func() error, error) {
	if path == "" {
		return os.Stdout, nil, nil
	}
	// #nosec G304 -- output path is provided via CLI flag.
	file, err := os.Create(path)
	if err != nil {
		return nil, nil, fmt.Errorf("create output file: %w", err)
	}
	writer := io.MultiWriter(os.Stdout, file)
	return writer, file.Close, nil
}

func writeTable(writer io.Writer, results []benchResult) error {
	tw := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "TARGET\tPROFILE\tSCENARIO\tRECORDS/S\tMB/S\tP50_MS\tP95_MS\tP99_MS\tDURATION_S\tRECORDS\tBYTES"); err != nil {
		return err
	}
	for _, result := range results {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%d\n",
			result.Target,
			result.Profile,
			result.Scenario,
			formatFloat(result.RecordsPerSec),
			formatFloat(result.MBPerSec),
			formatFloat(result.LatencyP50Ms),
			formatFloat(result.LatencyP95Ms),
			formatFloat(result.LatencyP99Ms),
			formatFloat(result.DurationSec),
			result.Records,
			result.Bytes,
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeMarkdown(writer io.Writer, results []benchResult) error {
	if _, err := fmt.Fprintln(writer, "| Target | Profile | Scenario | Records/s | MB/s | P50 ms | P95 ms | P99 ms | Duration s | Records | Bytes |"); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(writer, "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"); err != nil {
		return err
	}
	for _, result := range results {
		if _, err := fmt.Fprintf(
			writer,
			"| %s | %s | %s | %s | %s | %s | %s | %s | %s | %d | %d |\n",
			result.Target,
			result.Profile,
			result.Scenario,
			formatFloat(result.RecordsPerSec),
			formatFloat(result.MBPerSec),
			formatFloat(result.LatencyP50Ms),
			formatFloat(result.LatencyP95Ms),
			formatFloat(result.LatencyP99Ms),
			formatFloat(result.DurationSec),
			result.Records,
			result.Bytes,
		); err != nil {
			return err
		}
	}
	return nil
}

func writeBenchstat(writer io.Writer, results []benchResult) error {
	for _, result := range results {
		base := fmt.Sprintf(
			"BenchmarkCDC/%s/%s/%s",
			sanitizeBenchName(result.Target),
			sanitizeBenchName(result.Profile),
			sanitizeBenchName(result.Scenario),
		)
		if err := writeBenchstatMetric(writer, base, "records_per_sec", result.RecordsPerSec, "records/s"); err != nil {
			return err
		}
		if err := writeBenchstatMetric(writer, base, "mb_per_sec", result.MBPerSec, "MB/s"); err != nil {
			return err
		}
		if err := writeBenchstatMetric(writer, base, "latency_p50_ms", result.LatencyP50Ms, "ms"); err != nil {
			return err
		}
		if err := writeBenchstatMetric(writer, base, "latency_p95_ms", result.LatencyP95Ms, "ms"); err != nil {
			return err
		}
		if err := writeBenchstatMetric(writer, base, "latency_p99_ms", result.LatencyP99Ms, "ms"); err != nil {
			return err
		}
		if err := writeBenchstatMetric(writer, base, "duration_s", result.DurationSec, "s"); err != nil {
			return err
		}
	}
	return nil
}

func writeBenchstatMetric(writer io.Writer, base, metric string, value float64, unit string) error {
	_, err := fmt.Fprintf(writer, "%s/%s %s %s\n", base, metric, formatBenchFloat(value), unit)
	return err
}

func sanitizeBenchName(value string) string {
	if value == "" {
		return "unknown"
	}
	value = strings.ReplaceAll(value, " ", "_")
	value = strings.ReplaceAll(value, "\t", "_")
	return value
}

func writeJSON(writer io.Writer, results []benchResult) error {
	enc := json.NewEncoder(writer)
	enc.SetIndent("", "  ")
	return enc.Encode(results)
}

func formatFloat(value float64) string {
	if value == 0 {
		return "0"
	}
	return fmt.Sprintf("%.2f", value)
}

func formatBenchFloat(value float64) string {
	if value == 0 {
		return "0"
	}
	return fmt.Sprintf("%.6f", value)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
