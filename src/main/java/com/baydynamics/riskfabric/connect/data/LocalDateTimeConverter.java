package com.baydynamics.riskfabric.connect.data;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class LocalDateTimeConverter {
    public static final String LOGICAL_NAME = "com.baydynamics.riskfabric.avro.LocalDateTime";

    public static SchemaBuilder builder() {
        return SchemaBuilder.int64()
            .name(LOGICAL_NAME)
            .version(1);
    }

    public static Schema schema() {
        return builder().build();
    }

    public static LocalDateTime toLogical(Schema schema, Object value) {
        if (!(LOGICAL_NAME.equals(schema.name()))) {
            throw new DataException("Requested conversion of " + schema.name() + "object but the schema does not match.");
        }

        if (value != null) {
            return LocalDateTime.ofInstant(Instant.ofEpochMilli((long) value), ZoneOffset.UTC); // use UTC for offset since the contract is LocalDateTime
        }
        return null;
    }
}