# Conduit Connector Kafka

The Kafka connector is one of [Conduit](https://github.com/ConduitIO/conduit) builtin plugins. It provides both, a
source and a destination connector for [Apache Kafka](https://kafka.apache.org).

<!-- readmegen:description -->
## Source

A Kafka source connector is represented by a single consumer in a Kafka consumer
group. By virtue of that, a source's logical position is the respective
consumer's offset in Kafka. Internally, though, we're not saving the offset as
the position: instead, we're saving the consumer group ID, since that's all
which is needed for Kafka to find the offsets for our consumer.

A source is associated with a consumer group ID when it is first opened. Set
`groupID` to choose it; otherwise a random one is generated and kept in the
position.

## Destination

The destination connector writes records to Kafka.

### Output format

The output format can be adjusted using configuration options provided by the
connector SDK:

- `sdk.record.format`: used to choose the format
- `sdk.record.format.options`: used to configure the specifics of the chosen format

See [this article](https://conduit.io/docs/connectors/output-formats) for more
info on configuring the output format.

### Batching

Batching can also be configured using connector SDK provided options:

- `sdk.batch.size`: maximum number of records in batch before it gets written to
  the destination (defaults to 0, no batching)
- `sdk.batch.delay`: maximum delay before an incomplete batch is written to the
  destination (defaults to 0, no limit)

## Delivery semantics

These are the guarantees that the connector's CI checks with the SDK
acceptance suite v2 and a kill -9 test, and the conditions under which they
hold.

### Source guarantees

- **At-least-once.** The source commits a record's offset to its consumer group
  only after Conduit acknowledged the record. It never commits an offset it
  has not been acked for.
- **Commits are batched**, so a restart after a crash re-delivers records:
  offsets are committed after every `commitOffsetsSize` acks (default 1000),
  every `commitOffsetsDelay` (default 5s), and when the pipeline stops
  gracefully. After a crash (kill -9, power loss) the source re-delivers at
  most the records acked since the last commit, fewer than `commitOffsetsSize`
  when commits succeed. Setting `commitOffsetsSize` to 1 commits on every ack
  and lowers throughput.
- **Ordering:** records of one partition are delivered in offset order, so
  records with the same key (which Kafka keeps in one partition) arrive in the
  order they were written. Re-delivery after a restart repeats a suffix of each
  partition, in order. There is no order between partitions.
- **Resume uses the consumer group**, not the offset in the Conduit position.
  If the group's committed offsets are gone (the group was deleted, or the
  pipeline was stopped longer than the broker's `offsets.retention.minutes`,
  7 days by default), the source starts where `readFromBeginning` says: from
  the beginning of the topic (re-delivery), or from the end, which **skips the
  records written while the pipeline was stopped**. Use
  `readFromBeginning: true` where skipping is not acceptable.
- **Restart after a crash can pause.** The crashed instance stays a member of
  the consumer group until the broker's session timeout expires (45s by
  default), and the partitions it held are not consumed until then.

### Destination guarantees

- **At-least-once.** A record is acknowledged to Conduit only after the broker
  confirmed it. With `acks: all` (the default) that means the in-sync replicas
  have it. `acks: one` acknowledges after the leader alone has it, and a leader
  failure can lose acknowledged records; `acks: none` acknowledges without any
  confirmation. Only `acks: all` keeps the at-least-once guarantee.
- **Not idempotent.** A record that is replayed after a crash is written again.
  The idempotent producer (on with `acks: all`) removes duplicates from its own
  retries of a produce request, not from a record sent twice.
- **Ordering:** records with the same key go to the same partition (key hash)
  in the order Conduit wrote them, as long as the partition count of the topic
  does not change. One exception: if the broker rejects a record after it was
  enqueued (for example `MESSAGE_TOO_LARGE`), the later records of the same
  batch may already be written. Conduit is told the batch failed from that
  record on, and when the records are replayed the rejected record is stored
  after them, together with duplicates of them.
- **Writes can wait for an unreachable broker without a bound** when
  `deliveryTimeout` is 0 (the default). Cancelling the pipeline ends the wait
  and nacks the records; nothing unconfirmed is acknowledged.
- **Large records:** a record must fit in `batchBytes` (default 1000012 bytes)
  and in the topic's `max.message.bytes`, after the output format is applied.
  The default `opencdc/json` format base64-encodes raw payloads, which adds
  about a third. The acceptance suite writes 1 MiB records with `batchBytes`
  and the topic's `max.message.bytes` set to 4 MiB.<!-- /readmegen:description -->

## Source Configuration Parameters

<!-- readmegen:source.parameters.yaml -->
```yaml
version: 2.2
pipelines:
  - id: example
    status: running
    connectors:
      - id: example
        plugin: "kafka"
        type: source
        settings:
          # Servers is a list of Kafka bootstrap servers, which will be used to
          # discover all the servers in a cluster.
          # Type: string
          # Required: yes
          servers: ""
          # Topics is a comma separated list of Kafka topics to read from.
          # Type: string
          # Required: yes
          topics: ""
          # CACert is the Kafka broker's certificate.
          # Type: string
          # Required: no
          caCert: ""
          # ClientCert is the Kafka client's certificate.
          # Type: string
          # Required: no
          clientCert: ""
          # ClientID is a unique identifier for client connections established
          # by this connector.
          # Type: string
          # Required: no
          clientID: "conduit-connector-kafka"
          # ClientKey is the Kafka client's private key.
          # Type: string
          # Required: no
          clientKey: ""
          # CommitOffsetsDelay defines how often consumed offsets should be
          # committed.
          # Type: duration
          # Required: no
          commitOffsetsDelay: "5s"
          # CommitOffsetsSize defines the maximum number of consumed offsets to
          # be committed at a time.
          # Type: int
          # Required: no
          commitOffsetsSize: "1000"
          # GroupID defines the consumer group id.
          # Type: string
          # Required: no
          groupID: ""
          # InsecureSkipVerify defines whether to validate the broker's
          # certificate chain and host name. If 'true', accepts any certificate
          # presented by the server and any host name in that certificate.
          # Type: bool
          # Required: no
          insecureSkipVerify: "false"
          # ReadFromBeginning determines from whence the consumer group should
          # begin consuming when it finds a partition without a committed
          # offset. If this options is set to true it will start with the first
          # message in that partition.
          # Type: bool
          # Required: no
          readFromBeginning: "false"
          # RetryGroupJoinErrors determines whether the connector will
          # continually retry on group join errors.
          # Type: bool
          # Required: no
          retryGroupJoinErrors: "true"
          # Mechanism configures the connector to use SASL authentication. If
          # empty, no authentication will be performed.
          # Type: string
          # Required: no
          saslMechanism: ""
          # Password sets up the password used with SASL authentication.
          # Type: string
          # Required: no
          saslPassword: ""
          # Username sets up the username used with SASL authentication.
          # Type: string
          # Required: no
          saslUsername: ""
          # TLSEnabled defines whether TLS is needed to communicate with the
          # Kafka cluster.
          # Type: bool
          # Required: no
          tls.enabled: "false"
          # Maximum delay before an incomplete batch is read from the source.
          # Type: duration
          # Required: no
          sdk.batch.delay: "0"
          # Maximum size of batch before it gets read from the source.
          # Type: int
          # Required: no
          sdk.batch.size: "0"
          # Specifies whether to use a schema context name. If set to false, no
          # schema context name will be used, and schemas will be saved with the
          # subject name specified in the connector (not safe because of name
          # conflicts).
          # Type: bool
          # Required: no
          sdk.schema.context.enabled: "true"
          # Schema context name to be used. Used as a prefix for all schema
          # subject names. If empty, defaults to the connector ID.
          # Type: string
          # Required: no
          sdk.schema.context.name: ""
          # Whether to extract and encode the record key with a schema.
          # Type: bool
          # Required: no
          sdk.schema.extract.key.enabled: "false"
          # The subject of the key schema. If the record metadata contains the
          # field "opencdc.collection" it is prepended to the subject name and
          # separated with a dot.
          # Type: string
          # Required: no
          sdk.schema.extract.key.subject: "key"
          # Whether to extract and encode the record payload with a schema.
          # Type: bool
          # Required: no
          sdk.schema.extract.payload.enabled: "false"
          # The subject of the payload schema. If the record metadata contains
          # the field "opencdc.collection" it is prepended to the subject name
          # and separated with a dot.
          # Type: string
          # Required: no
          sdk.schema.extract.payload.subject: "payload"
          # The type of the payload schema.
          # Type: string
          # Required: no
          sdk.schema.extract.type: "avro"
```
<!-- /readmegen:source.parameters.yaml -->

## Destination Configuration Parameters

<!-- readmegen:destination.parameters.yaml -->
```yaml
version: 2.2
pipelines:
  - id: example
    status: running
    connectors:
      - id: example
        plugin: "kafka"
        type: destination
        settings:
          # Servers is a list of Kafka bootstrap servers, which will be used to
          # discover all the servers in a cluster.
          # Type: string
          # Required: yes
          servers: ""
          # Acks defines the number of acknowledges from partition replicas
          # required before receiving a response to a produce request. None =
          # fire and forget, one = wait for the leader to acknowledge the
          # writes, all = wait for the full ISR to acknowledge the writes.
          # Type: string
          # Required: no
          acks: "all"
          # BatchBytes limits the maximum size of a request in bytes before
          # being sent to a partition. This mirrors Kafka's max.message.bytes.
          # Type: int
          # Required: no
          batchBytes: "1000012"
          # CACert is the Kafka broker's certificate.
          # Type: string
          # Required: no
          caCert: ""
          # ClientCert is the Kafka client's certificate.
          # Type: string
          # Required: no
          clientCert: ""
          # ClientID is a unique identifier for client connections established
          # by this connector.
          # Type: string
          # Required: no
          clientID: "conduit-connector-kafka"
          # ClientKey is the Kafka client's private key.
          # Type: string
          # Required: no
          clientKey: ""
          # Compression set the compression codec to be used to compress
          # messages.
          # Type: string
          # Required: no
          compression: "snappy"
          # DeliveryTimeout for write operation performed by the Writer.
          # Type: duration
          # Required: no
          deliveryTimeout: "0s"
          # InsecureSkipVerify defines whether to validate the broker's
          # certificate chain and host name. If 'true', accepts any certificate
          # presented by the server and any host name in that certificate.
          # Type: bool
          # Required: no
          insecureSkipVerify: "false"
          # Mechanism configures the connector to use SASL authentication. If
          # empty, no authentication will be performed.
          # Type: string
          # Required: no
          saslMechanism: ""
          # Password sets up the password used with SASL authentication.
          # Type: string
          # Required: no
          saslPassword: ""
          # Username sets up the username used with SASL authentication.
          # Type: string
          # Required: no
          saslUsername: ""
          # TLSEnabled defines whether TLS is needed to communicate with the
          # Kafka cluster.
          # Type: bool
          # Required: no
          tls.enabled: "false"
          # Topic is the Kafka topic. It can contain a [Go
          # template](https://pkg.go.dev/text/template) that will be executed
          # for each record to determine the topic. By default, the topic is the
          # value of the `opencdc.collection` metadata field.
          # Type: string
          # Required: no
          topic: "{{ index .Metadata "opencdc.collection" }}"
          # Maximum delay before an incomplete batch is written to the
          # destination.
          # Type: duration
          # Required: no
          sdk.batch.delay: "0"
          # Maximum size of batch before it gets written to the destination.
          # Type: int
          # Required: no
          sdk.batch.size: "0"
          # Allow bursts of at most X records (0 or less means that bursts are
          # not limited). Only takes effect if a rate limit per second is set.
          # Note that if `sdk.batch.size` is bigger than `sdk.rate.burst`, the
          # effective batch size will be equal to `sdk.rate.burst`.
          # Type: int
          # Required: no
          sdk.rate.burst: "0"
          # Maximum number of records written per second (0 means no rate
          # limit).
          # Type: float
          # Required: no
          sdk.rate.perSecond: "0"
          # The format of the output record. See the Conduit documentation for a
          # full list of supported formats
          # (https://conduitdata.io/docs/using/connectors/configuration-parameters/output-format).
          # Type: string
          # Required: no
          sdk.record.format: "opencdc/json"
          # Options to configure the chosen output record format. Options are
          # normally key=value pairs separated with comma (e.g.
          # opt1=val2,opt2=val2), except for the `template` record format, where
          # options are a Go template.
          # Type: string
          # Required: no
          sdk.record.format.options: ""
          # Whether to extract and decode the record key with a schema.
          # Type: bool
          # Required: no
          sdk.schema.extract.key.enabled: "true"
          # Whether to extract and decode the record payload with a schema.
          # Type: bool
          # Required: no
          sdk.schema.extract.payload.enabled: "true"
```
<!-- /readmegen:destination.parameters.yaml -->

## How to build?

Run `make build` to build the connector.

## Testing

Run `make test` to run all the unit and integration tests. Tests require Docker
to be installed and running. The command will handle starting and stopping
docker containers for you.

Tests will run twice, once against an Apache Kafka instance and a second time
against a [Redpanda](https://github.com/redpanda-data/redpanda) instance.

The broker is published on `127.0.0.1:9092` under the compose project
`conduit-connector-kafka-<broker>`. To run next to another broker, pick a
different port: `make test-kafka KAFKA_PORT=19092`. Tests find the broker
through `KAFKA_TEST_SERVERS`, which the Makefile sets.

`TestAcceptanceV2` runs the SDK's [acceptance suite
v2](https://github.com/ConduitIO/conduit-connector-sdk/blob/main/docs/acceptance-tests-v2.md).
To get its report, pass a directory, then check the report:

```sh
make test-kafka ACCEPTANCE_REPORT_DIR=$PWD
scripts/check-acceptance-report.sh acceptance-report-kafka.json
```

CI does this for both brokers and uploads the reports as artifacts. The
connector claims source ordering per key, at most 1000 re-delivered records
after a restart (above the suite's default of 10, so the report lists a
`max_redelivery` waiver request), and a non-idempotent destination. It uses
the suite's process-death hook: the suite kills a source in-process, which
leaves its consumer group membership alive, and the hook closes the killed
source's client the way the end of its process would (see
`acceptance_v2_test.go`).

![scarf pixel](https://static.scarf.sh/a.png?x-pxid=713ea3ba-66e0-4130-bdd0-d7db4b8706a0)
