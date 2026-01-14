package protoembed

import "embed"

// Files embeds the wallaby proto definitions for schema registry use.
//
//go:embed wallaby/v1/*.proto
var Files embed.FS
