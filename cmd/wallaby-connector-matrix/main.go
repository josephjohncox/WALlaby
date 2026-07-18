package main

import (
	"fmt"
	"os"

	pgsource "github.com/josephjohncox/wallaby/connectors/sources/postgres"
	"github.com/josephjohncox/wallaby/internal/runner"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	fmt.Println("<!-- Generated from executable connector capability declarations. Do not edit. -->")
	fmt.Println()
	fmt.Println("# Connector support matrix")
	fmt.Println()
	fmt.Println("`maintained` requires restart, replay, schema-evolution, and integration contract evidence. `experimental` adapters are usable but have not passed every maintained gate. `placeholder` endpoints have no runtime adapter.")
	fmt.Println()
	fmt.Println("## Sources")
	fmt.Println()
	fmt.Println("| Connector | Mode | Status | Restart | Replay | Schema evolution | Integration |")
	fmt.Println("| --- | --- | --- | --- | --- | --- | --- |")
	printSource("postgres", "cdc", (&pgsource.Source{}).Capabilities())
	printSource("postgres", "backfill", (&pgsource.BackfillSource{}).Capabilities())
	fmt.Println()
	fmt.Println("## Destinations")
	fmt.Println()
	fmt.Println("| Connector | Status | Runtime | Transactional batch | Idempotent replay | Replay safe | Executes DDL | Lossy |")
	fmt.Println("| --- | --- | --- | --- | --- | --- | --- | --- |")
	contracts, err := runner.DestinationContracts()
	if err != nil {
		return err
	}
	for _, contract := range contracts {
		capabilities := contract.Capabilities
		fmt.Printf("| `%s` | %s | %s | %s | %s | %s | %s | %s |\n",
			contract.Type,
			capabilities.Support,
			yesNo(contract.Runtime),
			yesNo(capabilities.Delivery.TransactionalBatch),
			yesNo(capabilities.Delivery.IdempotentReplay),
			yesNo(capabilities.Delivery.ReplaySafe),
			yesNo(capabilities.Delivery.ExecutesDDL),
			yesNo(capabilities.Delivery.Lossy),
		)
	}
	fmt.Println()
	fmt.Println("These are guaranteed defaults. Options can reduce guarantees; startup validation resolves configured capabilities before execution.")
	return nil
}

func printSource(name, mode string, capabilities connector.Capabilities) {
	fmt.Printf("| `%s` | %s | %s | %s | %s | %s | %s |\n",
		name,
		mode,
		capabilities.Support,
		yesNo(capabilities.Evidence.Restart),
		yesNo(capabilities.Evidence.Replay),
		yesNo(capabilities.Evidence.SchemaEvolution),
		yesNo(capabilities.Evidence.Integration),
	)
}

func yesNo(value bool) string {
	if value {
		return "yes"
	}
	return "no"
}
