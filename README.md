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

### Headers

Each Kafka header is emitted as a metadata entry `kafka.header.<key>` holding
the header value. Metadata is a string map, so a key that repeats in one
record keeps its last value.

A header whose key or value is not valid UTF-8 is emitted unchanged, but a
record carrying it cannot be marshalled as an OpenCDC protobuf record: it
fails wherever records cross a protobuf boundary (a standalone connector or
processor, the API), and JSON encoders replace the invalid bytes with U+FFFD.
The source logs `KAFKA_LEGACY_HEADER_NOT_UTF8` at WARN once per header key,
with the key as base64 in `header_key_b64` (the value is never logged), and
reports the count as `kafka_source_legacy_header_not_utf8_total` in the logs
and at teardown. A future version will omit such entries by default, after
the deprecation period
([#319](https://github.com/ConduitIO/conduit-connector-kafka/issues/319)).

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
  about a third, so with the defaults the largest raw payload is about
  730 KiB (roughly 750000 bytes, less the record's key and metadata). Larger
  records need a higher `batchBytes` and a higher `max.message.bytes` on the
  topic (and on the broker, if its `message.max.bytes` is lower). The
  acceptance suite's 1 MiB records run with both set to 4 MiB.

### Acceptance suite claims

The acceptance suite v2 run (revision 2.0.1) claims per-key ordering for the
source, a redelivery bound of 1000 records after a restart, and a
non-idempotent destination.

- **Redelivery bound.** The suite's default bound is 10. The source's bound,
  read from the code, is `commitOffsetsSize`: offsets are committed
  synchronously once that many acks have accumulated, so at most
  `commitOffsetsSize`-1 acked records are uncommitted when the process dies.
  The suite's restart case and the kill -9 test did not exceed it, which
  shows it is not violated in those runs; it is not a measured maximum. The
  bound of 1000 holds only for `commitOffsetsSize` up to 1000 (the default).
  A larger value raises the bound with it, and such a configuration is
  outside what the certification covers.
- **Destination per-key ordering is not claimed**, and the suite's ordering
  case for the destination is skipped, because of the exception above: a
  record the broker rejects after it was enqueued is stored after later
  records of its batch when the batch is replayed.
- **Killing a source in-process.** The suite kills a source inside the test
  process, which leaves its consumer group membership alive. The test closes
  the killed source's client without committing, which releases its
  partitions. A real process death sends no LeaveGroup: the broker drops the
  member only when its session times out (see "Restart after a crash can
  pause" above). The kill -9 test covers a real process death.

## Mirroring (Kafka→Kafka)

To copy a topic faithfully, preserving key, value, headers, timestamp and
partition, set `emitMirrorMetadata: true` on the source and `mirror: true` on
the destination:

```yaml
version: 2.2
pipelines:
  - id: mirror
    status: running
    connectors:
      - id: source
        plugin: "kafka"
        type: source
        settings:
          servers: "localhost:9092"
          topics: "orders"
          readFromBeginning: "true"
          emitMirrorMetadata: "true"
      - id: destination
        plugin: "kafka"
        type: destination
        settings:
          servers: "localhost:9092"
          topic: "orders.mirror"
          mirror: "true"
```

`mirror` writes each record to the destination partition named by the source
partition, so a consumer of the copy sees the same per-partition order as the
source. Rules and caveats:

- The destination topic must already exist (mirror mode does not auto-create
  it) and have at least as many partitions as the source topic; otherwise the
  first record fails with `KAFKA_PARTITION_COUNT_MISMATCH`, and a record whose
  partition is missing fails with `KAFKA_PARTITION_OUT_OF_RANGE`. A
  destination with more partitions is allowed: the extra partitions stay
  empty, and key-hash locality is not preserved.
- `mirror` requires `acks: all` and rejects `deliveryTimeout` and a user-set
  `sdk.record.format`; it writes the record value bytes verbatim, bypassing
  the output format.
- Per-partition order is preserved until the first produce error after a
  record is enqueued (for example the broker rejects it); after that the
  partition may hold later records before an earlier one, and the connector
  logs a WARN with `kafka_destination_post_enqueue_errors_total`. The remedy
  is to re-seed the affected partition.
- A mirror pipeline must keep the default error policy (a DLQ with
  `window-size: 0` or a nack threshold above 0 turns a configuration error
  into silent record loss).
- Timestamp topic-config checks are not yet enforced: a destination topic
  with `message.timestamp.type=LogAppendTime`, a short `retention.ms`, or
  `message.timestamp.before/after.max.ms` bounds may rewrite, expire or
  reject preserved timestamps. The connector preserves the timestamp but
  does not yet fail on these topic configurations.

## Redpanda

The kafka connector is the Redpanda profile: the retired labs `redpanda`
connector is replaced by this connector pointed at a Redpanda cluster
(`servers` set to the Redpanda Kafka API address). CI runs the integration,
acceptance-v2 and kill -9 tests against both Apache Kafka and Redpanda. Mirror
mode works on both brokers; Redpanda does not return or enforce the
`message.timestamp.before/after.max.ms` bounds, so only the timestamp type and
retention rules can differ from Apache Kafka.

## Host-supplied dialer

If the host that runs the connector puts a dialer on the context
(`dialer.WithDialContext` from `conduit-commons`), the connector opens every
broker connection through it: the seed brokers, brokers discovered from
metadata, reconnects, and the admin and metadata requests. When TLS is
enabled, the handshake runs over the connection the host dialer returns.
Without a host dialer the connector dials directly, as before.<!-- /readmegen:description -->

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
          # EmitMirrorMetadata, when true, additionally emits lossless mirror
          # metadata on every record: kafka.partition, kafka.partitions,
          # kafka.offset, kafka.timestamp, kafka.timestamp.type, kafka.headers,
          # kafka.key.null and kafka.value.null. It is off by default because
          # metadata is carried to every destination, and destinations that
          # serialize it write different bytes for pipelines that never asked
          # for mirroring. Enable it together with the destination's mirror
          # option to preserve header, partition and timestamp across a
          # Kafka-to-Kafka copy.
          # Type: bool
          # Required: no
          emitMirrorMetadata: "false"
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
          # AllowPlaintext lets SASL authenticate over a connection that is not
          # protected by TLS (tls.enabled is false). This exposes the SASL
          # credentials, and with PLAIN the password itself, to anyone who can
          # observe the network path to the brokers. By default the connector
          # refuses to start in that configuration. Only set this for a trusted
          # network, such as a local development cluster.
          # Type: bool
          # Required: no
          saslAllowPlaintext: "false"
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
          # Maximum delay before an incomplete batch is read from the source. 0
          # returns what is already queued without waiting.
          # Type: duration
          # Required: no
          sdk.batch.delay: "0"
          # Maximum size of batch before it gets read from the source.
          # Type: int
          # Required: no
          sdk.batch.size: "100"
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
          # AllowLossyHeaders, valid only with mirror, writes headers rebuilt
          # from kafka.header.<k> entries when kafka.headers is absent (repeated
          # keys collapsed, order undefined). Without it, mirror mode fails a
          # record whose kafka.headers is missing.
          # Type: bool
          # Required: no
          allowLossyHeaders: "false"
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
          # Mirror enables Kafka-to-Kafka mirror mode: it writes the record's
          # key, value, headers, timestamp and partition to Kafka verbatim from
          # the mirror metadata the source emits when its emitMirrorMetadata
          # option is on. It requires acks=all, rejects deliveryTimeout > 0 and
          # a user-set sdk.record.format, and disables topic auto-creation. See
          # the kafka mirror passthrough design in ConduitIO/conduit.
          # Type: bool
          # Required: no
          mirror: "false"
          # ProduceTimeout is a connector-side bound on a Write in mirror mode,
          # because deliveryTimeout must stay unset there. Minimum 30s, maximum
          # 1h.
          # Type: duration
          # Required: no
          produceTimeout: "2m"
          # AllowPlaintext lets SASL authenticate over a connection that is not
          # protected by TLS (tls.enabled is false). This exposes the SASL
          # credentials, and with PLAIN the password itself, to anyone who can
          # observe the network path to the brokers. By default the connector
          # refuses to start in that configuration. Only set this for a trusted
          # network, such as a local development cluster.
          # Type: bool
          # Required: no
          saslAllowPlaintext: "false"
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
`max_redelivery` waiver; see "Acceptance suite claims" above), and a
non-idempotent destination. It uses
the suite's process-death hook: the suite kills a source in-process, which
leaves its consumer group membership alive, and the hook closes the killed
source's client without committing, which releases its partitions (see
`acceptance_v2_test.go`).

`make test-chaos-kafka` and `make test-chaos-redpanda` run the kill -9 test in
`test/chaos` (build tag `chaos`). It copies 20000 records from one topic to
another through this connector's source and destination in a child process,
kills the child with SIGKILL four times while it copies, and checks after each
kill that every committed source offset is already in the destination
(no early ack) and that committed offsets never move back. After the last run
it checks that every record arrived, that duplicates stay within the
redelivery bound, that per-key order holds, and that a graceful stop commits
everything. Each restart waits for the killed instance's consumer group
session to expire, so the test takes about five minutes.

![scarf pixel](https://static.scarf.sh/a.png?x-pxid=713ea3ba-66e0-4130-bdd0-d7db4b8706a0)
