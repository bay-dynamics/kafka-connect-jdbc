package com.baydynamics.riskfabric.connect.data;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;

public class UIDConverter {
    public static final String LOGICAL_NAME = "riskfabric.UID";

    public static SchemaBuilder builder() {
        return SchemaBuilder.string()
                .name(LOGICAL_NAME)
                .version(1);
    }

    public static Schema schema() {
        return builder().build();
    }

    public static java.util.UUID toLogical(Schema schema, Object value) {
        if (!(LOGICAL_NAME.equals(schema.name()))) {
            throw new DataException("Requested conversion of UUID object but the schema does not match.");
        }
        return java.util.UUID.fromString(value.toString());
    }
}