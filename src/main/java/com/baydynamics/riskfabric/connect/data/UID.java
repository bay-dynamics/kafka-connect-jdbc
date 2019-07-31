package com.baydynamics.riskfabric.connect.data;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;

public class UID {
    public static final String LOGICAL_NAME = "riskfabric.UID";

    public static SchemaBuilder builder() {
        return SchemaBuilder.string()
                .name(LOGICAL_NAME);
    }

    public static Schema schema() {
        return builder().build();
    }

    /**
     * Convert a value from its logical format (java.util.UUID) to it's encoded format.
     *
     * @param value the logical value
     * @return the encoded value
     */
    public static String fromLogical(Schema schema, java.util.UUID value) {
        if (!(LOGICAL_NAME.equals(schema.name()))) {
            throw new DataException("Requested conversion of UUID object but the schema does not match.");
        }
        return value.toString();
    }

    public static java.util.UUID toLogical(Schema schema, String value) {
        if (!(LOGICAL_NAME.equals(schema.name()))) {
            throw new DataException("Requested conversion of UUID object but the schema does not match.");
        }
        return java.util.UUID.fromString(value);
    }
}