# Conduit Connector Kafka
![scarf pixel](https://static.scarf.sh/a.png?x-pxid=713ea3ba-66e0-4130-bdd0-d7db4b8706a0)

The Kafka connector is one of [Conduit](https://github.com/ConduitIO/conduit) builtin plugins. It provides both, a
source and a destination connector for [Apache Kafka](https://kafka.apache.org).

## How to build?

Run `make build` to build the connector.

## Testing

Run `make test` to run all the unit and integration tests. Tests require Docker to be installed and running. The command
will handle starting and stopping docker containers for you.

Tests will run twice, once against an Apache Kafka instance and a second time against a
[Redpanda](https://github.com/redpanda-data/redpanda) instance.

## Source

A Kafka source connector is represented by a single consumer in a Kafka consumer group. By virtue of that, a source's
logical position is the respective consumer's offset in Kafka. Internally, though, we're not saving the offset as the
position: instead, we're saving the consumer group ID, since that's all which is needed for Kafka to find the offsets
for our consumer.

A source is getting associated with a consumer group ID the first time the `Read()` method is called.

### Configuration

| name                 | description                                                                                                                                                                                                  | required | default value             |
|----------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|---------------------------|
| `servers`            | Servers is a list of Kafka bootstrap servers, which will be used to discover all the servers in a cluster.                                                                                                   | true     |                           |
| `topics`             | Topics is a comma separated list of Kafka topics from which records will be read, ex: "topic1,topic2".                                                                                                       | true     |                           |
| ~~`topic`~~          | Topic is the Kafka topic to read from. **Deprecated: use `topics` instead.**                                                                                                                                 | false    |                           |
| `clientID`           | A Kafka client ID.                                                                                                                                                                                           | false    | `conduit-connector-kafka` |
| `readFromBeginning`  | Determines from whence the consumer group should begin consuming when it finds a partition without a committed offset. If this option is set to true it will start with the first message in that partition. | false    | `false`                   |
| `groupID`            | Defines the consumer group ID.                                                                                                                                                                               | false    |                           |
| `tls.enabled`        | Defines whether TLS is enabled.                                                                                                                                                                              | false    | `false`                   |
| `clientCert`         | A certificate for the Kafka client, in PEM format. If provided, the private key needs to be provided too.                                                                                                    | false    |                           |
| `clientKey`          | A private key for the Kafka client, in PEM format. If provided, the certificate needs to be provided too.                                                                                                    | false    |                           |
| `caCert`             | The Kafka broker's certificate, in PEM format.                                                                                                                                                               | false    |                           |
| `insecureSkipVerify` | Controls whether a client verifies the server's certificate chain and host name. If `true`, accepts any certificate presented by the server and any host name in that certificate.                           | false    | `false`                   |
| `saslMechanism`      | SASL mechanism to be used. Possible values: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512. If empty, authentication won't be performed.                                                                                | false    |                           |
| `saslUsername`       | SASL username. If provided, a password needs to be provided too.                                                                                                                                             | false    |                           |
| `saslPassword`       | SASL password. If provided, a username needs to be provided too.                                                                                                                                             | false    |                           |
| `retryGroupJoinErrors`       | determines whether the connector will continually retry on group join errors                                                                                                                                              | false    | `true` |

## Destination

The destination connector sends records to Kafka.

### Configuration

There's no global, connector configuration. Each connector instance is configured separately.

| name                 | description                                                                                                                                                                                                                                                          | required | default value                                |
|----------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|----------------------------------------------|
| `servers`            | Servers is a list of Kafka bootstrap servers, which will be used to discover all the servers in a cluster.                                                                                                                                                           | true     |                                              |
| `topic`              | Topic is the Kafka topic. It can contain a [Go template](https://pkg.go.dev/text/template) that will be executed for each record to determine the topic. By default, the topic is the value of the `opencdc.collection` metadata field.                              | false    | `{{ index .Metadata "opencdc.collection" }}` |
| `clientID`           | A Kafka client ID.                                                                                                                                                                                                                                                   | false    | `conduit-connector-kafka`                    |
| `acks`               | Acks defines the number of acknowledges from partition replicas required before receiving a response to a produce request. `none` = fire and forget, `one` = wait for the leader to acknowledge the writes, `all` = wait for the full ISR to acknowledge the writes. | false    | `all`                                        |
| `deliveryTimeout`    | Message delivery timeout.                                                                                                                                                                                                                                            | false    |                                              |
| `batchBytes`         | Limits the maximum size of a request in bytes before being sent to a partition. This mirrors Kafka's `max.message.bytes`.                                                                                                                                            | false    | 1000012                                      |
| `compression`        | Compression applied to messages. Possible values: `none`, `gzip`, `snappy`, `lz4`, `zstd`.                                                                                                                                                                           | false    | `snappy`                                     |
| `clientCert`         | A certificate for the Kafka client, in PEM format. If provided, the private key needs to be provided too.                                                                                                                                                            | false    |                                              |
| `clientKey`          | A private key for the Kafka client, in PEM format. If provided, the certificate needs to be provided too.                                                                                                                                                            | false    |                                              |
| `caCert`             | The Kafka broker's certificate, in PEM format.                                                                                                                                                                                                                       | false    |                                              |
| `insecureSkipVerify` | Controls whether a client verifies the server's certificate chain and host name. If `true`, accepts any certificate presented by the server and any host name in that certificate.                                                                                   | false    | `false`                                      |
| `saslMechanism`      | SASL mechanism to be used. Possible values: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512. If empty, authentication won't be performed.                                                                                                                                        | false    |                                              |
| `saslUsername`       | SASL username. If provided, a password needs to be provided too.                                                                                                                                                                                                     | false    |                                              |
| `saslPassword`       | SASL password. If provided, a username needs to be provided too.                                                                                                                                                                                                     | false    |                                              |

### Output format

The output format can be adjusted using configuration options provided by the connector SDK:

- `sdk.record.format`: used to choose the format
- `sdk.record.format.options`: used to configure the specifics of the chosen format

See [this article](https://conduit.io/docs/connectors/output-formats) for more info
on configuring the output format.

### Batching

Batching can also be configured using connector SDK provided options:

- `sdk.batch.size`: maximum number of records in batch before it gets written to the destination (defaults to 0, no batching)
- `sdk.batch.delay`: maximum delay before an incomplete batch is written to the destination (defaults to 0, no limit)
