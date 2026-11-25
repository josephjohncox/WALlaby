# Apache Iggy destination

`iggy` publishes CDC batches through Apache Iggy's native TCP protocol. It does
not use Kafka or require a Kafka-compatible gateway. Kafka and Redpanda remain
separate destination types.

The connector uses the official Go SDK `v0.10.0`, paired with Iggy server
`0.10.0`. Older pre-VSR SDK/server releases are not compatible with this client.
See the [Iggy Go SDK reference](https://iggy.apache.org/docs/0.10/sdk/go/intro/).

## Delivery contract

The adapter is **experimental** and append-only. One encoded CDC batch becomes
one message, sent synchronously to an explicit, zero-based partition. The default
partition is `0`. Pre-create the stream, topic and partition; Open does not
provision or delete broker resources.

A successful Write means the SDK acknowledged the send. Iggy's topic durability
policy determines whether that acknowledgement includes disk persistence. Choose
`persisted` durability when a broker crash must not lose acknowledged data.
An empty placement list is still an SDK acknowledgement, not a reason to resend.

If the SDK reports an uncertain operation, the connector retains its operation
ID and accepts only a retry of the same logical batch. It resolves the original
request instead of submitting a fresh send. Another batch cannot overtake that
operation. Context/client-state errors and unclassified failures keep the retained
request. For a lost recovery identity or an allowlisted authorization/resource
refusal, the connector explicitly abandons local ownership and returns an error;
the next retry starts a fresh **at-least-once** send, which can duplicate an old
commit. Failed abandonment preserves pending state. No error path acknowledges
or silently skips the batch. Unclassified permanent failures require operator
intervention/restart rather than an unproved resend.

This retained state is process-local. A crash after broker commit but before the
flow checkpoint can produce a duplicate on restart. Stable message IDs and the
batch-hash header are correlation/deduplication inputs for consumers, **not an
exactly-once guarantee**. The connector does not advertise idempotent replay,
transactional batches or replay safety and cannot satisfy primary-ack outbox
requirements. Use `ack_policy: all` with an appropriate consumer deduplication
policy. Neither this connector nor a broker acknowledgement replaces a durable
WALlaby checkpoint.

OpenTelemetry spans cover Open, Write and Close. Counters
`wallaby.iggy.messages.acknowledged` and `wallaby.iggy.write.errors` report
acknowledged batch messages and failed write/resolution attempts. They do not
claim that an acknowledged message survived a broker or worker crash.

## Typed endpoint

```yaml
name: iggy_out
iggy:
  address: iggy.example.test:8090
  stream: cdc
  topic: orders
  partition_id: 0
  token: replace-with-a-personal-access-token
  tls: true
  tls_server_name: iggy.example.test
  timeout: 10s
  max_message_bytes: 1000000
  format: WIRE_FORMAT_JSON
```

Use either `token` or both `username` and `password`, never both methods.
Credentials have no built-in defaults. TLS is enabled by default and certificate
verification cannot be disabled. `tls_ca_file` supplies a custom CA file;
`tls_server_name` overrides SNI/hostname verification. These options are rejected
when TLS is disabled. Set `tls: false` only for an explicitly plaintext broker,
such as the disposable localhost fixture below.

| Field | Meaning/default |
| --- | --- |
| `address` | Required `host:port`, without a URL scheme or embedded credentials |
| `stream`, `topic` | Required resource **names**, each 1–255 bytes; numeric-looking names are not IDs |
| `partition_id` | Unsigned, zero-based partition; default `0` |
| `format` | Arrow (default), Avro OCF, Protobuf or JSON; Parquet is not supported |
| `timeout` | Positive connect/send/resolution timeout up to the SDK limit of `30s`; default `10s`, bounded by caller cancellation |
| `max_message_bytes` | Encoded payload limit; default 1,000,000 bytes, maximum 64,000,000 |
| `type_mappings.mappings` | Optional source-type mapping overrides, as for other event transports |

All records in a batch stay in the same message. Oversized payloads fail before
sending; there is no lossy skip or implicit split. Reduce the source batch size
or raise the limit within the broker's configured request limit.

Each message carries `wallaby-format`, `wallaby-content-type` and
`wallaby-batch-hash` user headers. The payload uses WALlaby's shared wire codecs.
This initial connector does not expose Kafka keys, Kafka transactions, Kafka
record mode or schema-registry options. It transports event/schema information;
it does not execute downstream DDL or perform current-state table upserts.
Set `config.ddl.auto_apply: false` for an Iggy flow: the shipped generic DDL
default requires execution, which this transport deliberately cannot provide.

## Local fixture and integration test

The fixture uses public development credentials, loopback-only port exposure and
no persistent volume. It requires Docker with a Linux kernel of at least 5.19;
see [Iggy's container requirements](https://iggy.apache.org/docs/0.10/server/docker/).

```bash
docker compose -f examples/iggy/docker-compose.yml up -d --wait --wait-timeout 120
WALLABY_TEST_IGGY_ADDRESS=localhost:8090 \
WALLABY_TEST_IGGY_USERNAME=iggy \
WALLABY_TEST_IGGY_PASSWORD='WallabyLocal123!' \
WALLABY_TEST_IGGY_TLS=false \
mise run test-iggy
```

The test creates an isolated stream, publishes and reads back all four formats,
checks message headers and removes its own stream. `test-iggy` requires explicit
connection settings and rejects skipped/missing test evidence. The separate
`CI Iggy / iggy-native` job runs `mise run test-live-local`: Go executors own
kind PostgreSQL and an isolated Compose Iggy project, verify all formats plus
PostgreSQL-to-Iggy CDC/checkpoint restart, and require no-skip JSON evidence.
See [live delivery harnesses](../development/live-delivery.md).

For a flow, create stream `cdc` and topic `orders` with at least one partition
using Iggy's administrative CLI or SDK, then use
[`examples/flows/postgres_to_iggy.yaml`](https://github.com/josephjohncox/WALlaby/blob/main/examples/flows/postgres_to_iggy.yaml).
That example's plaintext credentials are only for the local fixture.

```bash
docker compose -f examples/iggy/docker-compose.yml down
```

Unit tests exercise configuration, encoding, cancellation, size bounds, failed
Open cleanup and uncertain-request recovery. Real-service readback is separate
from those tests and does not establish crash-safe replay or a maintained support
classification.
