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
	fmt.Println("| Connector | Status | Runtime | Append mapping | Explicit-key upsert | Watermark guard | Transactional batch | Idempotent replay | Replay safe | Executes DDL | Reconciles DDL | Lossy |")
	fmt.Println("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
	contracts, err := runner.DestinationContracts()
	if err != nil {
		return err
	}
	for _, contract := range contracts {
		capabilities := contract.Capabilities
		fmt.Printf("| `%s` | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n",
			contract.Type, capabilities.Support, yesNo(contract.Runtime),
			yesNo(capabilities.TableWrites.Append), yesNo(capabilities.TableWrites.Upsert && capabilities.TableWrites.ExplicitKey), yesNo(capabilities.TableWrites.WatermarkGuard),
			yesNo(capabilities.Delivery.TransactionalBatch),
			yesNo(capabilities.Delivery.IdempotentReplay),
			yesNo(capabilities.Delivery.ReplaySafe),
			yesNo(capabilities.Delivery.ExecutesDDL),
			yesNo(contract.ReconcilesDDL),
			yesNo(capabilities.Delivery.Lossy),
		)
	}
	fmt.Println()
	fmt.Println("Snowpipe is append-only staged delivery: PUT, optional COPY, and metadata-receipt errors are returned unchanged; target tables change only through configured COPY or external pipe ingestion.")
	fmt.Println()
	fmt.Println("## Configuration-controlled capability profiles")
	fmt.Println()
	fmt.Println("| Connector | Profile | Append | Explicit-key upsert | Watermark guard | Transactional batch | Idempotent replay | Replay safe | Executes DDL | Lossy |")
	fmt.Println("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
	for _, registration := range runner.DestinationRegistrations() {
		if registration.New == nil {
			continue
		}
		destination := registration.New()
		for _, profile := range registration.Profiles {
			spec := connector.RuntimeSpec{Name: string(registration.Type), Type: registration.Type, Options: profile.Options}
			capabilities, err := registration.ResolveCapabilities(destination, spec)
			if err != nil {
				return err
			}
			fmt.Printf("| `%s` | `%s` | %s | %s | %s | %s | %s | %s | %s | %s |\n",
				registration.Type, profile.ID,
				yesNo(capabilities.TableWrites.Append), yesNo(capabilities.TableWrites.Upsert && capabilities.TableWrites.ExplicitKey), yesNo(capabilities.TableWrites.WatermarkGuard),
				yesNo(capabilities.Delivery.TransactionalBatch), yesNo(capabilities.Delivery.IdempotentReplay), yesNo(capabilities.Delivery.ReplaySafe),
				yesNo(capabilities.Delivery.ExecutesDDL), yesNo(capabilities.Delivery.Lossy))
		}
	}
	fmt.Println()
	fmt.Println("## Managed profiles")
	fmt.Println()
	profiles := []connector.ManagedProfileContract{
		connector.PostgresToPostgresV1Profile(),
		connector.PostgresToClickHouseAppendV1Profile(),
		connector.PostgresToSnowflakeSQLV1Profile(),
		connector.PostgresToSnowflakeStagedAppendV1Profile(),
		connector.PostgresToSnowflakeStreamingRestAppendV1Profile(),
	}
	fmt.Println("| Profile | Status | Source | Destination | PostgreSQL | ClickHouse | Snowflake version | Deployment | Pairing | Ack | Sinks | Delivery | Table mappings |")
	fmt.Println("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
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
		snowflakeVersion := strings.Join(profile.SnowflakeVersions, ", ")
		if snowflakeVersion == "" && profile.SnowflakeVersionPolicy != "" {
			snowflakeVersion = profile.SnowflakeVersionPolicy + " (reviewed versions: none)"
		}
		if snowflakeVersion == "" {
			snowflakeVersion = "—"
		}
		deployment := profile.Deployment
		if len(profile.SnowflakeDeploymentCells) > 0 {
			deployment += " [" + strings.Join(profile.SnowflakeDeploymentCells, ", ") + "]"
		} else if profile.Destination == connector.EndpointSnowflake {
			deployment += " [reviewed cells: none]"
		}
		if deployment == "" {
			deployment = "—"
		}
		pairing := "mixed majors"
		if profile.SameMajorOnly {
			pairing = "same major"
		} else if profile.Destination == connector.EndpointSnowflake {
			pairing = "configured runtime pin; unreviewed"
		}
		fmt.Printf("| `%s` | %s | `%s` | `%s` | %s | %s | %s | %s | %s | %s | one | %s | %s |\n", profile.Name, profile.Support, profile.Source, profile.Destination, strings.Join(versions, ", "), clickHouseVersions, snowflakeVersion, deployment, pairing, strings.Join(profile.AckPolicies, ", "), profile.DeliveryGuarantee, managedProfileMappings(profile.Name))
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
	fmt.Println("These are declared defaults. Options can reduce guarantees; startup validation resolves configured capabilities before execution. Generic PostgreSQL, ClickHouse, Snowflake, and Snowpipe modes remain experimental. Maintained status applies only to rows explicitly marked maintained. The Snowflake SQL, staged COPY append, and Streaming append rows are implemented modeled protocol profiles, not promoted support claims: SQL and staged COPY still lack a reviewed Snowflake service version/deployment cell with every unskipped same-SHA live recovery gate, while Streaming additionally has no linked reviewed append transport and therefore fails closed before external I/O.")
	return nil
}

func managedProfileMappings(name string) string {
	switch name {
	case connector.ManagedProfilePostgresToPostgresV1:
		return "append; explicit-key upsert; watermark guard"
	case connector.ManagedProfilePostgresToClickHouseAppendV1:
		return "append only"
	case connector.ManagedProfilePostgresToSnowflakeSQLV1:
		return "one exact relation; explicit-key upsert; complete source PK; future tables excluded; no watermark"
	default:
		return "—"
	}
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
