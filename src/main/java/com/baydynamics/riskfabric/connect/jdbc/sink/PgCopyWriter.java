package com.baydynamics.riskfabric.connect.jdbc.sink;

import com.baydynamics.riskfabric.connect.jdbc.dialect.RiskFabricDatabaseDialect;
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
import java.util.List;
import java.util.Optional;

public class PgCopyWriter extends GenericDbWriter {
    private static final Logger log = LoggerFactory.getLogger(PgCopyWriter.class);

    private TableId tableId;
    private String consumerGroupId;
    private String topicName;
    private Integer[] partitionIds;

    private boolean doOffsetTracking = false;

    HashMap<TopicPartition, Long> dbOffsetMap = new HashMap<>();

    public PgCopyWriter(final RiskFabricJdbcSinkConfig config, RiskFabricDatabaseDialect dbDialect, DbStructure dbStructure) throws ConnectException
    {
        super(config, dbDialect, dbStructure);
    }

    public PgCopyWriter trackOffsets(String groupId, Collection<TopicPartition> partitionAssignements) {
        if (!doOffsetTracking) {
            consumerGroupId = groupId;

            // there is at least one by design
            Optional<TopicPartition> first = partitionAssignements.stream().findFirst();

            topicName = first.get().topic();
            tableId = destinationTable(topicName);

            partitionIds = new Integer[partitionAssignements.size()];
            int i = 0;
            for (TopicPartition topicPartition : partitionAssignements) {
                partitionIds[i++] = topicPartition.partition();
            }

            dbOffsetMap = new HashMap<>();

            // read or initialize offsets for partitionAssignements
            final Connection connection = cachedConnectionProvider.getConnection();
            dbOffsetMap = PgRoutines.getOrInitializeTopicPartitions(connection, consumerGroupId, tableId.schemaName(), tableId.tableName(), topicName, partitionIds);

            // safeguard check
            for (TopicPartition partition : partitionAssignements) {
                if (!dbOffsetMap.containsKey(partition)) {
                    String msg = String.format("initialization error: topic partition %s-%d not found for sink table %s.", partition.topic(), partition.partition(), tableId.schemaName() != null ? tableId.schemaName() + "." : "" , tableId.tableName());
                    throw new ConnectException(msg);
                }
            }

            doOffsetTracking = true;
        }
        return this;
    }

    public HashMap<TopicPartition,Long> getOffsetMap() {
        if (doOffsetTracking) {
            if (dbOffsetMap == null) {
                final Connection connection = cachedConnectionProvider.getConnection();
                dbOffsetMap = PgRoutines.getOrInitializeTopicPartitions(connection, consumerGroupId, tableId.schemaName(), tableId.tableName(), topicName, partitionIds);
            }

            return dbOffsetMap;
        }

        throw new ConnectException(String.format("%s is not set to track offsets.", PgCopyWriter.class));
    }

    public void write(final Collection<SinkRecord> records) throws SQLException {
        final Connection connection = cachedConnectionProvider.getConnection();

        PgCopyBuffer copyStatement = null;
        for (SinkRecord record : records) {
            if (copyStatement == null) {
                final TableId tableId = destinationTable(record.topic());
                copyStatement = new PgCopyBuffer((RiskFabricJdbcSinkConfig)config, tableId, (RiskFabricDatabaseDialect)dbDialect, dbStructure, connection)
                {
                    protected void onFlush(List<SinkRecord> flushedRecords, HashMap<Integer, Long> flushedOffsets) throws SQLException {

                        if (doOffsetTracking) {
                            // persist the offsets
                            Integer[] ids = new Integer[flushedOffsets.size()];
                            Long[] newOffsets = new Long[flushedOffsets.size()];
                            int i = 0;
                            for (Integer id : flushedOffsets.keySet()) {
                                ids[i] = id;
                                // INCREMENT PERSISTED OFFSET BY 1 AS THE NEXT STARTING POSITION
                                newOffsets[i] = flushedOffsets.get(id) + 1;
                                i++;
                            }

                            // persist to database
                            dbOffsetMap = PgRoutines.updateWatermarkOffsets(connection, consumerGroupId, tableId.schemaName(), tableId.tableName(), topicName, ids, newOffsets);
                        }

                        connection.commit(); // checkpoint in transaction

                        // after this point if doOffsetTracking=true and the connector fails or kafka fails
                        // the connector may be restarted from the offsets

                        log.info(String.format("batch with %d records committed.", flushedRecords.size()));
                    }
                };
            }
            copyStatement.add(record);
        }

        copyStatement.flush();
    }
}