//go:build ignore

package main

import (
	"fmt"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

func main() {
	profiles := []connector.ManagedProfileContract{
		connector.PostgresToPostgresV1Profile(),
		connector.PostgresToClickHouseAppendV1Profile(),
		connector.PostgresToSnowflakeSQLV1Profile(),
	}
	for _, profile := range profiles {
		if profile.Support != connector.SupportMaintained {
			continue
		}
		for _, gate := range profile.Gates {
			fmt.Printf("%s|%s\n", profile.Name, gate.Test)
		}
	}
}
