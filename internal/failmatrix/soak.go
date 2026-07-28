package failmatrix

import (
	"runtime"
	"time"
)

// SoakSample is a periodic resource observation during a soak run.
type SoakSample struct {
	ElapsedMS      int64  `json:"elapsed_ms"`
	Cycles         int    `json:"cycles"`
	Goroutines     int    `json:"goroutines"`
	HeapInuseBytes uint64 `json:"heap_inuse_bytes"`
	Passed         int    `json:"passed"`
	Failed         int    `json:"failed"`
}

// SoakReport is the machine-readable result of a bounded soak run.
type SoakReport struct {
	Seed              int64         `json:"seed"`
	DurationRequested string        `json:"duration_requested"`
	DurationActualMS  int64         `json:"duration_actual_ms"`
	TotalCycles       int           `json:"total_cycles"`
	Passed            int           `json:"passed"`
	Failed            int           `json:"failed"`
	FailClosedCycles  int           `json:"fail_closed_cycles"`
	GoroutineStart    int           `json:"goroutine_start"`
	GoroutineEnd      int           `json:"goroutine_end"`
	GoroutineMax      int           `json:"goroutine_max"`
	HeapStartBytes    uint64        `json:"heap_start_bytes"`
	HeapEndBytes      uint64        `json:"heap_end_bytes"`
	HeapMaxBytes      uint64        `json:"heap_max_bytes"`
	GoroutineGrowthOK bool          `json:"goroutine_growth_ok"`
	NoViolations      bool          `json:"no_violations"`
	Samples           []SoakSample  `json:"samples"`
	Violations        []CycleResult `json:"violations,omitempty"`
}

// Ok reports whether the soak run passed every gate: no invariant violations,
// and bounded goroutine growth (no leak).
func (r SoakReport) Ok() bool {
	return r.NoViolations && r.GoroutineGrowthOK
}

// SoakConfig parameterizes a bounded soak run.
type SoakConfig struct {
	Duration       time.Duration
	Seed           int64
	SampleInterval time.Duration
	// GoroutineGrowthLimit bounds acceptable end-vs-start goroutine growth.
	GoroutineGrowthLimit int
	Profiles             []Profile
}

// Soak repeatedly drives the failure matrix for a bounded wall-clock budget and
// records resource growth. It runs entirely in process with clean ownership: no
// goroutines are spawned, so a leak would indicate a defect in the code under
// test rather than the harness. It asserts no invariant violations and bounded
// goroutine growth. Live-service soak for the exact maintained profiles remains
// a separate, opt-in recipe.
func Soak(cfg SoakConfig) SoakReport {
	if cfg.Duration <= 0 {
		cfg.Duration = 30 * time.Second
	}
	if cfg.SampleInterval <= 0 {
		cfg.SampleInterval = cfg.Duration / 10
		if cfg.SampleInterval <= 0 {
			cfg.SampleInterval = time.Second
		}
	}
	if cfg.GoroutineGrowthLimit <= 0 {
		cfg.GoroutineGrowthLimit = 8
	}
	profiles := cfg.Profiles
	if len(profiles) == 0 {
		profiles = SupportedProfiles()
	}
	boundaries := RequiredBoundaries()
	faults := []FaultKind{FaultKill, FaultRestart, FaultOverlappingTakeover}

	runtime.GC()
	startGoroutines := runtime.NumGoroutine()
	var memStart runtime.MemStats
	runtime.ReadMemStats(&memStart)

	report := SoakReport{
		Seed:              cfg.Seed,
		DurationRequested: cfg.Duration.String(),
		GoroutineStart:    startGoroutines,
		GoroutineMax:      startGoroutines,
		HeapStartBytes:    memStart.HeapInuse,
		HeapMaxBytes:      memStart.HeapInuse,
	}

	start := time.Now()
	deadline := start.Add(cfg.Duration)
	nextSample := start.Add(cfg.SampleInterval)
	cycleID := 0
	// A deterministic sequence of (profile, boundary, fault) tuples keyed by the
	// running cycle count and seed.
	for time.Now().Before(deadline) {
		p := profiles[cycleID%len(profiles)]
		b := boundaries[(cycleID/len(profiles))%len(boundaries)]
		f := faults[cycleID%len(faults)]
		cycleSeed := deriveSeed(cfg.Seed, p.Name, string(b)) ^ int64(cycleID)
		result := RunCycle(cycleID, cycleSeed, p, b, f)
		cycleID++
		report.TotalCycles++
		if result.FailClosed {
			report.FailClosedCycles++
		}
		if result.Ok() {
			report.Passed++
		} else {
			report.Failed++
			if len(report.Violations) < 64 {
				report.Violations = append(report.Violations, result)
			}
		}

		if !time.Now().Before(nextSample) {
			g := runtime.NumGoroutine()
			var mem runtime.MemStats
			runtime.ReadMemStats(&mem)
			if g > report.GoroutineMax {
				report.GoroutineMax = g
			}
			if mem.HeapInuse > report.HeapMaxBytes {
				report.HeapMaxBytes = mem.HeapInuse
			}
			report.Samples = append(report.Samples, SoakSample{
				ElapsedMS:      time.Since(start).Milliseconds(),
				Cycles:         report.TotalCycles,
				Goroutines:     g,
				HeapInuseBytes: mem.HeapInuse,
				Passed:         report.Passed,
				Failed:         report.Failed,
			})
			nextSample = nextSample.Add(cfg.SampleInterval)
		}
	}

	runtime.GC()
	endGoroutines := runtime.NumGoroutine()
	var memEnd runtime.MemStats
	runtime.ReadMemStats(&memEnd)
	report.DurationActualMS = time.Since(start).Milliseconds()
	report.GoroutineEnd = endGoroutines
	report.HeapEndBytes = memEnd.HeapInuse
	if endGoroutines > report.GoroutineMax {
		report.GoroutineMax = endGoroutines
	}
	report.GoroutineGrowthOK = endGoroutines-startGoroutines <= cfg.GoroutineGrowthLimit
	report.NoViolations = report.Failed == 0
	return report
}
