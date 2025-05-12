/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.bindings.kafka;

import org.apache.kafka.connect.data.Struct;

public interface DebeziumSinkRecord extends io.debezium.sink.DebeziumSinkRecord {

    Struct kafkaCoordinates();

    Struct kafkaHeader();

}
