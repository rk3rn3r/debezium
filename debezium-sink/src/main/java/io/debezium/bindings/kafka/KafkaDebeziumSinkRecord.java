/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.bindings.kafka;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;

import io.debezium.annotation.Immutable;
import io.debezium.data.Envelope;
import io.debezium.sink.SinkConnectorConfig;
import io.debezium.sink.field.FieldDescriptor;
import io.debezium.sink.filter.FieldFilterFactory;
import io.debezium.util.Strings;

@Immutable
public class KafkaDebeziumSinkRecord implements DebeziumSinkRecord {

    protected final SinkRecord originalKafkaRecord;
    private final Struct kafkaCoordinates;
    private final Struct kafkaPayload;
    private final Struct kafkaHeader;

    protected final Map<String, FieldDescriptor> allFields = new LinkedHashMap<>();

    public KafkaDebeziumSinkRecord(SinkRecord record) {
        Objects.requireNonNull(record, "The sink record must be provided.");

        this.originalKafkaRecord = record;
        this.kafkaCoordinates = getKafkaCoordinates();
        this.kafkaHeader = getRecordHeaders();

        final var flattened = isFlattened();
        // final boolean truncated = !flattened && isTruncate();
        if (flattened || !isTruncate()) {
            this.kafkaPayload = getKafkaPayload(flattened);
        }
        else {
            this.kafkaPayload = null;
        }
    }

    @Override
    public String topicName() {
        return originalKafkaRecord.topic();
    }

    @Override
    public Integer partition() {
        return originalKafkaRecord.kafkaPartition();
    }

    @Override
    public long offset() {
        return originalKafkaRecord.kafkaOffset();
    }

    @Override
    public Struct kafkaCoordinates() {
        return kafkaCoordinates;
    }

    @Override
    public Struct kafkaHeader() {
        return kafkaHeader;
    }

    public Map<String, FieldDescriptor> allFields() {
        return allFields;
    }

    @Override
    public Object key() {
        return originalKafkaRecord.key();
    }

    @Override
    public Schema keySchema() {
        return originalKafkaRecord.keySchema();
    }

    public Object value() {
        return originalKafkaRecord.value();
    }

    @Override
    public Schema valueSchema() {
        return originalKafkaRecord.valueSchema();
    }

    @Override
    public boolean isDebeziumMessage() {
        return originalKafkaRecord.value() != null && originalKafkaRecord.valueSchema() != null && originalKafkaRecord.valueSchema().name() != null
                && originalKafkaRecord.valueSchema().name().endsWith("Envelope");
    }

    public boolean isSchemaChange() {
        return originalKafkaRecord.valueSchema() != null
                && !Strings.isNullOrEmpty(originalKafkaRecord.valueSchema().name())
                && originalKafkaRecord.valueSchema().name().contains(SCHEMA_CHANGE_VALUE);
    }

    public boolean isFlattened() {
        return !isTombstone() && null != originalKafkaRecord.valueSchema()
                && (originalKafkaRecord.valueSchema().name() == null || !originalKafkaRecord.valueSchema().name().contains("Envelope"));
    }

    @Override
    public boolean isTombstone() {
        // NOTE
        // Debezium TOMBSTONE has both value and valueSchema to null, instead of the ExtractNewRecordState SMT with delete.handling.mode=none
        // which will generate a record with value null that should be treated as a flattened delete. See isDelete method.
        return originalKafkaRecord.value() == null && originalKafkaRecord.valueSchema() == null;
    }

    @Override
    public boolean isDelete() {
        if (!isDebeziumMessage()) {
            return originalKafkaRecord.value() == null;
        }
        else if (originalKafkaRecord.value() != null) {
            final Struct value = (Struct) originalKafkaRecord.value();
            return Envelope.Operation.DELETE.equals(Envelope.Operation.forCode(value.getString(Envelope.FieldName.OPERATION)));
        }
        return false;
    }

    @Override
    public boolean isTruncate() {
        if (isDebeziumMessage()) {
            Struct value = (Struct) originalKafkaRecord.value();
            if (value.schema().name().endsWith("CloudEvents.Envelope")) {
                value = value.getStruct("data");
            }
            return Envelope.Operation.TRUNCATE.equals(Envelope.Operation.forCode(value.getString(Envelope.FieldName.OPERATION)));
        }
        return false;
    }

    public Struct getPayload() {
        return kafkaPayload;
    }

    public Struct getFilteredPayload(FieldFilterFactory.FieldNameFilter fieldsFilter) {
        return filterFields(getPayload(), topicName(), Set.of(), fieldsFilter);
    }

    private Struct filterFields(Struct data, String topicName, Set<String> allowedPrimaryKeyFields, FieldFilterFactory.FieldNameFilter fieldsFilter) {
        final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
        if (!allowedPrimaryKeyFields.isEmpty()) {
            allowedPrimaryKeyFields.forEach(fieldName -> {
                Field field = data.schema().field(fieldName);
                schemaBuilder.field(field.name(), field.schema());
            });
            final Schema schema = schemaBuilder.build();
            Struct filteredData = new Struct(schema);
            schema.fields().forEach(field -> filteredData.put(field.name(), data.get(field.name())));
            return filteredData;
        }
        else {
            data.schema().fields().forEach(field -> {
                if (null != fieldsFilter) {
                    if (fieldsFilter.matches(topicName, field.name())) {
                        schemaBuilder.field(field.name(), field.schema());
                    }
                }
                else {
                    schemaBuilder.field(field.name(), field.schema());
                }
            });
            final Schema schema = schemaBuilder.build();
            Struct filteredData = new Struct(schema);
            schema.fields().forEach(field -> {
                if (null != fieldsFilter) {
                    if (fieldsFilter.matches(topicName, field.name())) {
                        filteredData.put(field.name(), data.get(field));
                    }
                }
                else {
                    filteredData.put(field.name(), data.get(field));
                }
            });
            return filteredData;
        }
    }

    @Override
    public Struct getFilteredKey(SinkConnectorConfig.PrimaryKeyMode primaryKeyMode, Set<String> allowedPrimaryKeyFields,
                                 FieldFilterFactory.FieldNameFilter fieldsFilter) {
        switch (primaryKeyMode) {
            case RECORD_KEY -> {
                final Schema keySchema = keySchema();
                if (keySchema == null) {
                    throw new ConnectException("Configured primary key mode 'record_key' cannot have null schema");
                }
                else if (keySchema.type().isPrimitive()) {
                    return kafkaKeyStruct();
                }
                if (Schema.Type.STRUCT.equals(keySchema.type())) {
                    return filterFields(kafkaKeyStruct(), topicName(), allowedPrimaryKeyFields, fieldsFilter);
                }
                else {
                    throw new ConnectException("An unsupported record key schema type detected: " + keySchema.type());
                }
            }
            case RECORD_VALUE -> {
                final Schema valueSchema = originalKafkaRecord.valueSchema();
                if (valueSchema != null && Schema.Type.STRUCT.equals(valueSchema.type())) {
                    return filterFields(getPayload(), topicName(), allowedPrimaryKeyFields, fieldsFilter);
                }
                else {
                    throw new ConnectException("No struct-based primary key defined for record value");
                }
            }
            case RECORD_HEADER -> {
                if (originalKafkaRecord.headers().isEmpty()) {
                    throw new ConnectException("Configured primary key mode 'record_header' cannot have empty message headers");
                }
                return getRecordHeaders();
            }
        }
        return null;
    }

    public SinkRecord getOriginalKafkaRecord() {
        return originalKafkaRecord;
    }

    private static final String KAFKA_COORDINATES = "__kafka_coordinates";
    private static final String CONNECT_TOPIC = "__connect_topic";
    private static final String CONNECT_PARTITION = "__connect_partition";
    private static final String CONNECT_OFFSET = "__connect_offset";

    public static final Schema KAFKA_COORDINATES_SCHEMA = SchemaBuilder.struct().name(KAFKA_COORDINATES)
            .field(CONNECT_TOPIC, Schema.OPTIONAL_STRING_SCHEMA)
            .field(CONNECT_PARTITION, Schema.OPTIONAL_INT32_SCHEMA)
            .field(CONNECT_OFFSET, Schema.OPTIONAL_INT64_SCHEMA).build();

    private Struct getKafkaCoordinates() {
        Struct kafkaCoordinatesStruct = new Struct(KAFKA_COORDINATES_SCHEMA);
        kafkaCoordinatesStruct
                .put(CONNECT_TOPIC, originalKafkaRecord.topic())
                .put(CONNECT_PARTITION, originalKafkaRecord.kafkaPartition())
                .put(CONNECT_OFFSET, originalKafkaRecord.kafkaOffset());

        allFields.put(CONNECT_TOPIC, new FieldDescriptor(Schema.STRING_SCHEMA, CONNECT_TOPIC));
        allFields.put(CONNECT_PARTITION, new FieldDescriptor(Schema.INT32_SCHEMA, CONNECT_PARTITION));
        allFields.put(CONNECT_OFFSET, new FieldDescriptor(Schema.INT64_SCHEMA, CONNECT_OFFSET));

        return kafkaCoordinatesStruct;
    }

    private Struct kafkaKeyStruct() {
        if (keySchema() != null) {
            return (Struct) originalKafkaRecord.key();
        }
        return null;
    }

    private static final String KAFKA_HEADERS_SUFFIX = "$__kafka_headers";

    private Struct getRecordHeaders() {
        final SchemaBuilder headerSchemaBuilder = SchemaBuilder.struct().name(originalKafkaRecord.topic() + KAFKA_HEADERS_SUFFIX);
        originalKafkaRecord.headers().forEach((Header header) -> headerSchemaBuilder.field(header.key(), header.schema()));
        final Schema headerSchema = headerSchemaBuilder.build();

        final Struct headerStruct = new Struct(headerSchema);
        originalKafkaRecord.headers().forEach((Header header) -> headerStruct.put(header.key(), header.value()));

        return headerStruct;
    }

    private Struct getKafkaPayload(boolean flattened) {
        final Schema valueSchema = valueSchema();
        if (valueSchema == null) {
            return null;
        }
        if (flattened) {
            return (Struct) value();
        }
        else {
            if (valueSchema.name().endsWith("CloudEvents.Envelope")) {
                return ((Struct) value()).getStruct("data").getStruct(Envelope.FieldName.AFTER);
            }
            return ((Struct) value()).getStruct(Envelope.FieldName.AFTER);
        }

        // if (!configuredPrimaryKeyFields.isEmpty()) {
        // recordFields = recordFields.filter(field -> configuredPrimaryKeyFields.contains(field.name()));
        // }
    }

}
