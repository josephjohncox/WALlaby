package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

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
	fmt.Println("| Connector | Status | Runtime | Transactional batch | Idempotent replay | Replay safe | Executes DDL | Reconciles DDL | Lossy |")
	fmt.Println("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
	contracts, err := runner.DestinationContracts()
	if err != nil {
		return err
	}
	for _, contract := range contracts {
		capabilities := contract.Capabilities
		fmt.Printf("| `%s` | %s | %s | %s | %s | %s | %s | %s | %s |\n",
			contract.Type,
			capabilities.Support,
			yesNo(contract.Runtime),
			yesNo(capabilities.Delivery.TransactionalBatch),
			yesNo(capabilities.Delivery.IdempotentReplay),
			yesNo(capabilities.Delivery.ReplaySafe),
			yesNo(capabilities.Delivery.ExecutesDDL),
			yesNo(contract.ReconcilesDDL),
			yesNo(capabilities.Delivery.Lossy),
		)
	}
	fmt.Println()
	fmt.Println("## Managed profiles")
	fmt.Println()
	profiles := []connector.ManagedProfileContract{
		connector.PostgresToPostgresV1Profile(),
		connector.PostgresToClickHouseAppendV1Profile(),
	}
	fmt.Println("| Profile | Status | Source | Destination | PostgreSQL | ClickHouse | Deployment | Pairing | Ack | Sinks | Delivery |")
	fmt.Println("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
	for _, profile := range profiles {
		if err := profile.ValidatePromotion(); err != nil {
			return err
		}
		versions := make([]string, 0, len(profile.PostgresVersions))
		for _, version := range profile.PostgresVersions {
			versions = append(versions, strconv.Itoa(version))
		}
		clickHouseVersions := strings.Join(profile.ClickHouseVersions, ", ")
		if clickHouseVersions == "" {
			clickHouseVersions = "—"
		}
		deployment := profile.Deployment
		if deployment == "" {
			deployment = "—"
		}
		pairing := "mixed majors"
		if profile.SameMajorOnly {
			pairing = "same major"
		}
		fmt.Printf("| `%s` | %s | `%s` | `%s` | %s | %s | %s | %s | %s | one | %s |\n",
			profile.Name, profile.Support, profile.Source, profile.Destination,
			strings.Join(versions, ", "), clickHouseVersions, deployment, pairing,
			strings.Join(profile.AckPolicies, ", "), profile.DeliveryGuarantee)
	}
	for _, profile := range profiles {
		fmt.Println()
		fmt.Printf("### `%s` evidence gates\n", profile.Name)
		fmt.Println()
		fmt.Println("| Admission/evidence gate | Real service | Required test |")
		fmt.Println("| --- | --- | --- |")
		for _, gate := range profile.Gates {
			fmt.Printf("| %s | %s | `%s` |\n", gate.Capability, yesNo(gate.Live), gate.Test)
		}
	}
	fmt.Println()
	fmt.Println("These are guaranteed defaults. Options can reduce guarantees; startup validation resolves configured capabilities before execution. Generic PostgreSQL and ClickHouse modes remain experimental; maintained status applies only to the exact named managed profiles above.")
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
