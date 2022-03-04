### General
The Kafka connector is one of [Conduit](https://github.com/ConduitIO/conduit) builtin plugins.
It provides both, a source and a destination Kafka connector.

### How to build it
Run `make`.

### Testing
Run `make test` to run all the unit and integration tests, which require Docker to be installed
and running. The command will handle starting and stopping docker containers for you.

### How it works
Under the hood, the connector uses [Segment's Go Client for Apache Kafka(tm)](https://github.com/segmentio/kafka-go). It was 
chosen since it has no CGo dependency, making it possible to build the connector for a wider range of platforms and architectures.
It also supports contexts, which will likely use in the future.

#### Source
A Kafka source connector is represented by a single consumer in a Kafka consumer group. By virtue of that, a source's 
logical position is the respective consumer's offset in Kafka. Internally, though, we're not saving the offset as the 
position: instead, we're saving the consumer group ID, since that's all which is needed for Kafka to find the offsets for
our consumer.

A source is getting associated with a consumer group ID the first time the `Read()` method is called.

#### Destination
The destination connector uses **synchronous** writes to Kafka. Proper buffering support which will enable asynchronous 
(and more optimal) writes is planned.

### Configuration
There's no global, connector configuration. Each connector instance is configured separately. 

| name                 | part of             | description                                                                                                                                                                        | required | default value |
|----------------------|---------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|---------------|
| `servers`            | destination, source | A list of bootstrap servers to which the plugin will connect.                                                                                                                      | true     |               |
| `topic`              | destination, source | The topic to which records will be written to.                                                                                                                                     | true     |               |
| `acks`               | destination         | The number of acknowledgments required before considering a record written to Kafka. Valid values: 0, 1, all                                                                       | false    | `all`         |
| `deliveryTimeout`    | destination         | Message delivery timeout.                                                                                                                                                          | false    | `10s`         |
| `readFromBeginning`  | destination         | Whether or not to read a topic from beginning (i.e. existing messages or only new messages).                                                                                       | false    | `false`       |
| `clientCert`         | destination, source | A certificate for the Kafka client, in PEM format. If provided, the private key needs to be provided too.                                                                          | false    |               |
| `clientKey`          | destination, source | A private key for the Kafka client, in PEM format. If provided, the certificate needs to be provided too.                                                                          | false    |               |
| `caCert`             | destination, source | The Kafka broker's certificate, in PEM format.                                                                                                                                     | false    |               |
| `insecureSkipVerify` | destination, source | Controls whether a client verifies the server's certificate chain and host name. If `true`, accepts any certificate presented by the server and any host name in that certificate. | false    | `false`       |
