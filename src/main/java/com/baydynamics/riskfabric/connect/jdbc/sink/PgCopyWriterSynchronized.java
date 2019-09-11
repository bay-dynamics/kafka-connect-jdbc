package com.baydynamics.riskfabric.connect.jdbc.sink;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.sink.DbStructure;
import io.confluent.connect.jdbc.util.TableId;
import io.confluent.connect.jdbc.sink.GenericDbWriter;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Optional;

public class PgCopyWriterSynchronized extends GenericDbWriter {
    private static final Logger log = LoggerFactory.getLogger(PgCopyWriterSynchronized.class);

    private Iterable<SinkTableState> state;
    private Collection<TopicPartition> partitions;

    public PgCopyWriterSynchronized(final RiskFabricJdbcSinkConfig config, DatabaseDialect dbDialect, DbStructure dbStructure, Collection<TopicPartition> partitionAssignments) throws ConnectException
    {
        super(config, dbDialect, dbStructure);

        this.partitions = partitionAssignments;
    }

    public HashMap<TopicPartition,Long> getOffsetMaps() {
        final Connection connection = cachedConnectionProvider.getConnection();

        // there is at least one by design
        Optional<TopicPartition> first = partitions.stream().findFirst();
        final TableId tableId = destinationTable(first.get().topic());

        Object[] partitionIds = new Object[partitions.size()];
        int i = 0;
        for (TopicPartition topicPartition : partitions) {
            partitionIds[i++] = topicPartition.partition();
        }

        HashMap<TopicPartition,Long> dbOffsetMap=new HashMap<>();

        state = SinkTableRoutines.getOrInitializeTopicPartitionState(connection, tableId.schemaName(), tableId.tableName(), first.get().topic(), partitionIds);
        for (SinkTableState partitionState : state) {
            dbOffsetMap.put(new TopicPartition(partitionState.kafkaTopic(), partitionState.kafkaPartition()), partitionState.tableKafkaOffset());
        }

        if (dbOffsetMap.size() != partitions.size()) {
            String msg = String.format("sink table %s.%s state has a different number of partitions (%d) than the current task (%d).", tableId.schemaName(), tableId.tableName(), dbOffsetMap.size(), partitions.size());
            throw new ConnectException(msg);
        }
        else {
            for (TopicPartition partition : dbOffsetMap.keySet()) {
                if (!partitions.contains(partition)) {
                    String msg = String.format("sink table topic partition %s-%d not found in the current task topic partition assignements.", partition.topic(), partition.partition());
                    throw new ConnectException(msg);
                }
            }
            return dbOffsetMap;
        }
    }

    private void writeOffsetMaps() {
        for (SinkTableState partitionState : state) {

        }
    }

    public void write(final Collection<SinkRecord> records) throws SQLException {
        final Connection connection = cachedConnectionProvider.getConnection();

        // track offsets
        HashMap latestOffsetByPartition = new HashMap<Integer, Long>();
        partitions.forEach((p) -> latestOffsetByPartition.put(p.partition(), null));

        PgCopy copyStatement = null;
        for (SinkRecord record : records) {
            if (copyStatement == null) {
                final TableId tableId = destinationTable(record.topic());
                copyStatement = new PgCopy((RiskFabricJdbcSinkConfig)config, tableId, dbDialect, dbStructure, connection);
            }
            copyStatement.add(record);
            latestOffsetByPartition.replace(record.kafkaPartition(), record.kafkaOffset());
        }

        copyStatement.close();



        connection.commit();
    }
}