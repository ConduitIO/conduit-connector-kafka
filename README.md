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

A source is getting associated with a consumer group ID the first time the
`Read()` method is called.

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
  destination (defaults to 0, no limit)<!-- /readmegen:description -->

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
          # CommitOffsetsDelay defines on how often consumed offsets should be
          # commited.
          # Type: duration
          # Required: no
          commitOffsetsDelay: "5s"
          # CommitOffsetsSize defines the number of consumed offsets to be
          # committed at a time.
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
          # (https://conduit.io/docs/using/connectors/configuration-parameters/output-format).
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

![scarf pixel](https://static.scarf.sh/a.png?x-pxid=713ea3ba-66e0-4130-bdd0-d7db4b8706a0)
