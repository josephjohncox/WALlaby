# Local benchmark evidence

These files are smoke evidence from the PeerDB/WALlaby gap implementation on
2026-07-20. They are not a comparative baseline and do not establish that
WALlaby is faster than PeerDB or PeerDB PR #4262.

## Environment

- Base commit: `4dec003be0b968db92ddcab4859ec6e0566b71e0`
- Repository state: uncommitted implementation work on top of the base commit
- Go tool: `go1.26.5 darwin/arm64`
- OS/architecture: Darwin arm64
- CPU: Apple M5 Max
- Repetitions: 10 per benchmark

## Commands

```bash
go test ./pkg/connector -run '^$' \
  -bench '^BenchmarkBatchContentHash$' -benchmem -count=10

go test ./pkg/stream -run '^$' \
  -bench '^BenchmarkWriteWithRetryOneFailureAmong(Four|Eight)$' \
  -benchmem -count=10

go test ./connectors/destinations/s3 -run '^$' \
  -bench '^BenchmarkDestinationWrite$' -benchmem -count=10
```

`implementation_writer_20260720.txt` is the Go benchmark output.
`implementation_writer_20260720.json` normalizes benchmark name, iterations,
nanoseconds per operation, bytes per operation, and allocations per operation.

The evidence does not include a PeerDB baseline, latency percentiles, peak RSS,
recovery time, cost, or soak duration. Performance claims require the broader
matrix recorded in `TODO.md` plus baseline/candidate metadata and statistical
comparison.
