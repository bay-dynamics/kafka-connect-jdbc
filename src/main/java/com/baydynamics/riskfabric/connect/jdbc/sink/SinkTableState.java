package com.baydynamics.riskfabric.connect.jdbc.sink;

import java.util.UUID;

public class SinkTableState {
    private UUID uid;
    private String schemaName;
    private String tableName;
    private String kafkaTopic;
    private int kafkaPartition;
    private long tableKafkaOffset;

    public UUID uid() {
        return uid;
    }

    public String schemaName() {
        return schemaName;
    }

    public String tableName() {
        return tableName;
    }

    public String kafkaTopic() {
        return kafkaTopic;
    }

    public int kafkaPartition() {
        return kafkaPartition;
    }

    public long tableKafkaOffset() {
        return tableKafkaOffset;
    }

    public void setUid(UUID value) {
        uid = value;
    }

    public void setSchemaName(String value) {
        schemaName = value;
    }

    public void setTableName(String value) {
        tableName = value;
    }

    public void setKafkaTopic(String value) {
        kafkaTopic = value;
    }

    public void setKafkaPartition(int value) {
        kafkaPartition = value;
    }

    public void setTableKafkaOffset(long value) {
        tableKafkaOffset = value;
    }
}