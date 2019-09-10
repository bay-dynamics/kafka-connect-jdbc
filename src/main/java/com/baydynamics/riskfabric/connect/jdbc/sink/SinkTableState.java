package com.baydynamics.riskfabric.connect.jdbc.sink;

import java.util.UUID;

public class SinkTableState {
    UUID kafkaConnectSinkTableStateUid;
    String schemaName;
    String tableName;
    String kafkaTopic;
    int kafkaPartition;
    long tableKafkaOffset;
}
