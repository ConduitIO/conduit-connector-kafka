#!/bin/bash
# This forces Kafka to set up the __consumer_offsets topic, which is done on first read.
# Without this, certain tests timeout, since the first read takes a lot of time.
echo "test-message" | kafka-console-producer --bootstrap-server localhost:9092 --topic test-topic
kafka-console-consumer --bootstrap-server localhost:9092 --topic test-topic --max-messages 1 --from-beginning
