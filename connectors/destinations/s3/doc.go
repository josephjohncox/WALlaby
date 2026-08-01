// Package s3 writes immutable CDC batch objects to S3-compatible storage.
//
// Stable object identities make exact in-memory batch retries converge and reject
// conflicting content at one object identity. The adapter remains at-least-once:
// source replay can form different batch boundaries after a crash, so the package
// does not claim restart-safe or idempotent replay.
package s3
