package com.baydynamics.riskfabric.connect.jdbc.sink;

import com.baydynamics.riskfabric.connect.jdbc.dialect.RiskFabricDatabaseDialect;
import com.baydynamics.riskfabric.connect.jdbc.util.CircularFifoHashSet;

import io.confluent.connect.jdbc.sink.DbStructure;
import io.confluent.connect.jdbc.util.TableId;
import io.confluent.connect.jdbc.sink.GenericDbWriter;
import io.confluent.connect.jdbc.util.StringUtils;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class PgCopyWriter extends GenericDbWriter {
    private static final Logger log = LoggerFactory.getLogger(PgCopyWriter.class);

    private TableId tableId;
    private String consumerGroupId;
    private String topicName;
    private Integer[] partitionIds;

    private boolean doOffsetTracking = false;
    private boolean doDedupKey = false;
    private String[] keyFields;

    private HashMap<TopicPartition, Long> dbOffsetMap;
    private CircularFifoHashSet<String> dedupKeyCache;
    RiskFabricJdbcSinkConfig config;

    //Offset management
    private HashMap<Integer, Long> offsetByPartition = new HashMap<Integer, Long>();

    public PgCopyWriter(final RiskFabricJdbcSinkConfig config, RiskFabricDatabaseDialect dbDialect, DbStructure dbStructure) throws ConnectException
    {
        super(config, dbDialect, dbStructure);

        this.config = config;
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

    public PgCopyWriter dedupRecords(String bufferQuery, int bufferSize, String[] keyFields) {
        if (StringUtils.isBlank(bufferQuery) || bufferSize <= 0 || keyFields == null) {
            throw new IllegalArgumentException();
        }

        if (!doDedupKey) {
            this.dedupKeyCache = new CircularFifoHashSet<>(bufferSize);
            final Connection connection = cachedConnectionProvider.getConnection();
            PgRoutines.executeQuery(connection, bufferQuery, (record) -> {
                try {
                    this.dedupKeyCache.appendWithNoCheck(extractKey(record, keyFields));
                }
                catch (SQLException sqle) {
                    throw new ConnectException(sqle);
                }
            });

            this.keyFields = keyFields;
            this.doDedupKey = true;
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

        Collection<SinkRecord> recordsToPush = records;
        if (doDedupKey) {
            Collection<SinkRecord> uniqueRecords = new ArrayList<SinkRecord>();
            for (SinkRecord record : records) {
                boolean added = dedupKeyCache.addIfAbsent(extractKey(record, keyFields));
                if (added) {
                    uniqueRecords.add(record);
                }
                else if (doOffsetTracking){
                    // dupe, we still want to shift the offset
                    if (offsetByPartition.containsKey(record.kafkaPartition())) {
                        offsetByPartition.replace(record.kafkaPartition(), record.kafkaOffset());
                    }
                    else {
                        offsetByPartition.put(record.kafkaPartition(), record.kafkaOffset());
                    }
                }
            }
            recordsToPush = uniqueRecords;
        }

        PgCopyBuffer copyStatement = null;
        for (SinkRecord record : recordsToPush) {
            if (copyStatement == null) {
                final TableId tableId = destinationTable(record.topic());
                copyStatement = new PgCopyBuffer((RiskFabricJdbcSinkConfig)config, tableId, (RiskFabricDatabaseDialect)dbDialect, dbStructure, connection)
                {
                    protected void onFlush(List<SinkRecord> flushedRecords) throws SQLException {

                        if (doOffsetTracking) {
                            // persist the offsets
                            Integer[] ids = new Integer[offsetByPartition.size()];
                            Long[] newOffsets = new Long[offsetByPartition.size()];
                            int i = 0;
                            for (Integer id : offsetByPartition.keySet()) {
                                ids[i] = id;
                                // INCREMENT PERSISTED OFFSET BY 1 AS THE NEXT STARTING POSITION
                                newOffsets[i] = offsetByPartition.get(id) + 1;
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

        if (copyStatement != null) {
            copyStatement.flush();
        }
    }

    public String extractKey(ResultSet record, String[] keyFieldNames) throws SQLException {
        StringBuilder key = new StringBuilder();
        for (int i = 0; i < keyFieldNames.length; i++) {
            if (i > 0) {
                key.append("-");
            }

            String fieldName = keyFieldNames[i];
            if (config.columnCaseType == RiskFabricJdbcSinkConfig.ColumnCaseType.SNAKE_CASE) {
                fieldName = StringUtils.toSnakeCase(keyFieldNames[i]);
            }

            String fieldValue = record.getString(fieldName);
            key.append(fieldValue != null ? fieldValue.toLowerCase() : "null");
        }
        return key.toString();
    }

    public String extractKey(SinkRecord record, String[] keyFieldNames) {
        StringBuilder key = new StringBuilder();
        Struct recordValue = (Struct) record.value();
        for (int i = 0; i < keyFieldNames.length; i++) {
            if (i > 0) {
                key.append("-");
            }

            String fieldValue = (String) recordValue.get(keyFieldNames[i]);
            key.append(fieldValue != null ? fieldValue.toLowerCase() : "null");
        }
        return key.toString();
    }
}