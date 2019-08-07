package com.baydynamics.riskfabric.connect.data;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.data.Struct;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

public class TimestampConverter {
    public static final String LOGICAL_NAME = "riskfabric.Timestamp";

    public static SchemaBuilder builder() {
        return SchemaBuilder.string()
                .name(LOGICAL_NAME);
    }

    public static TimestampRF toLogical(Schema schema, Object value) {
        if (!(LOGICAL_NAME.equals(schema.name()))) {
            throw new DataException(String.format("Requested conversion to %s. Expected: %s", schema.name(), LOGICAL_NAME));
        }

        Struct timestampAvro = (Struct)value;
        TimestampRF timestampRF = new TimestampRF();


        String sourceValue = timestampAvro.getString("SourceValue");
        Long utcDateEpoch = timestampAvro.getInt64("UtcDate");
        Integer offsetInMinutes = timestampAvro.getInt32("OffsetMinutes");
        String timeZoneName = timestampAvro.getString("TimeZoneName");



        return timestampRF;
    }
}