#!/bin/sh

/opt/kafka/bin/kafka-storage.sh format --config /opt/kafka/config/kraft/server.properties --cluster-id \
jtuuizedRyqvVKJohNNSBA --ignore-formatted

/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft/server.properties
