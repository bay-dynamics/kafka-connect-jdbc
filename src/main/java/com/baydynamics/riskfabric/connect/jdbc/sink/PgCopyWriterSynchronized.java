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

    private Iterable<SinkTableState> initialState;
    private Collection<TopicPartition> partitions;
    private TableId tableId;
    private String topicName;
    private Integer[] partitionIds;

    public PgCopyWriterSynchronized(final RiskFabricJdbcSinkConfig config, DatabaseDialect dbDialect, DbStructure dbStructure, Collection<TopicPartition> partitionAssignments) throws ConnectException
    {
        super(config, dbDialect, dbStructure);

        this.partitions = partitionAssignments;

        // there is at least one by design
        Optional<TopicPartition> first = partitions.stream().findFirst();
        topicName = first.get().topic();
        tableId = destinationTable(topicName);

        partitionIds = new Integer[partitions.size()];
        int i = 0;
        for (TopicPartition topicPartition : partitions) {
            partitionIds[i++] = topicPartition.partition();
        }

        initialState = getState();
    }

    private Iterable<SinkTableState> getState() {
        final Connection connection = cachedConnectionProvider.getConnection();
        return SinkTableRoutines.getOrInitializeTopicPartitionState(connection, tableId.schemaName(), tableId.tableName(), topicName, partitionIds);
    }

    public HashMap<TopicPartition,Long> getNextOffsetMaps() {
        HashMap<TopicPartition,Long> offsetMaps = getOffsetMaps();
        // INCREMENT PERSISTED OFFSET BY 1 AS THE NEXT RECOVERY STARTING POSITION
        offsetMaps.replaceAll((t,offset) -> (offset <= 0 ? 0 : offset + 1));
        return offsetMaps;
    }

    public HashMap<TopicPartition,Long> getOffsetMaps() {
        HashMap<TopicPartition,Long> dbOffsetMap=new HashMap<>();

        for (SinkTableState partitionState : getState()) {
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

    public void write(final Collection<SinkRecord> records) throws SQLException {
        final Connection connection = cachedConnectionProvider.getConnection();

        // track offsets
        HashMap<Integer, Long> latestOffsetByPartition = new HashMap<Integer, Long>();

        PgCopy copyStatement = null;
        for (SinkRecord record : records) {
            if (copyStatement == null) {
                final TableId tableId = destinationTable(record.topic());
                copyStatement = new PgCopy((RiskFabricJdbcSinkConfig)config, tableId, dbDialect, dbStructure, connection);
            }
            copyStatement.add(record);

            if (latestOffsetByPartition.containsKey(record.kafkaPartition())) {
                latestOffsetByPartition.replace(record.kafkaPartition(), record.kafkaOffset());
            }
            else {
                latestOffsetByPartition.put(record.kafkaPartition(), record.kafkaOffset());
            }
        }

        copyStatement.close();

        // persist the offsets
        Integer[] ids = new Integer[latestOffsetByPartition.size()];
        Long[] newOffsets = new Long[latestOffsetByPartition.size()];
        int i = 0;
        for (Integer id : latestOffsetByPartition.keySet()) {
            ids[i] = id;
            newOffsets[i] = latestOffsetByPartition.get(id); //TODO do I need to increment by 1?
            i++;
        }

        SinkTableRoutines.updateTopicPartitionState(connection, tableId.schemaName(), tableId.tableName(), topicName, ids, newOffsets);

        connection.commit(); // end of the transaction

        // after this point, if the connector fails or kafka fails to commit its offset for the consumer group
        // the connector may be restated and it would resynchronize from the persisted offset and carry on!
    }
}