package com.baydynamics.riskfabric.connect.data;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class EpochMillisConverter {
    public static final String LOGICAL_NAME = "riskfabric.EPOCH_MILLIS";

    public static SchemaBuilder builder() {
        return SchemaBuilder.string()
                .name(LOGICAL_NAME);
    }

    public static Schema schema() {
        return builder().build();
    }

    public static LocalDateTime toLogical(Schema schema, Object value) {
        if (!(LOGICAL_NAME.equals(schema.name()))) {
            throw new DataException("Requested conversion of EpochMillis object but the schema does not match.");
        }

        return LocalDateTime.ofInstant(Instant.ofEpochMilli((long) value), ZoneOffset.UTC); // no offset, epochs are produced as local timestamp
    }
}