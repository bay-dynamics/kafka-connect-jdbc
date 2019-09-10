package com.baydynamics.riskfabric.connect.jdbc.sink;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.common.TopicPartition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Collection;

public class SinkTableStateManager {
    private static final Logger log = LoggerFactory.getLogger(SinkTableStateManager.class);

    public static HashMap<TopicPartition,Long> getOrInitializeTopicPartitionAssignments(Connection conn, String schema, String tableName, String topicName, Collection<TopicPartition> topicPartitions) throws ConnectException
    {
        Object[] partitionIds = new Object[topicPartitions.size()];
        int i = 0;
        for (TopicPartition topicPartition : topicPartitions) {
            partitionIds[i++] = topicPartition.partition();
        }

        HashMap<TopicPartition,Long> dbOffsetMap=new HashMap<>();

        // get the persisted state
        PreparedStatement statement = null;
        try {
            statement = conn.prepareStatement("SELECT * FROM get_or_initialize_kafka_connect_sink_table_state(?, ?, ?, ?)");
            statement.setString(1, schema);
            statement.setString(2, tableName);
            statement.setString(3, topicName);
            statement.setArray(4, conn.createArrayOf("integer", partitionIds));

            ResultSet rs = statement.executeQuery();
            if (rs.isBeforeFirst()) {//if state is not empty
                while (rs.next()) {
                    dbOffsetMap.put(new TopicPartition(rs.getString("kafka_topic"), rs.getInt("kafka_partition")), rs.getLong("table_kafka_offset"));
                }
                if (dbOffsetMap.size() != topicPartitions.size()) {
                    String msg = String.format("sink table %s.%s state has a different number of partitions (%d) than the current task (%d).", schema, tableName, dbOffsetMap.size(), topicPartitions.size());
                    throw new ConnectException(msg);
                }
                else {
                    for (TopicPartition partition : dbOffsetMap.keySet()) {
                        if (!topicPartitions.contains(partition)) {
                            String msg = String.format("sink table topic partition %s-%d not found in the current task topic partition assignements.", partition.topic(), partition.partition());
                            throw new ConnectException(msg);
                        }
                    }

                    return dbOffsetMap;
                }
            }
            else
            {
                String msg = String.format("Unable to get kafka connect sink table state for %s.%s and topic %s", schema, tableName, topicName);
                log.error("get_or_initialize_kafka_connect_sink_table_state returned nothing.");
                throw new ConnectException(msg);
            }
        }
        catch (SQLException sqle) {
            throw new ConnectException(sqle);
        }
        finally {
            if (statement != null) {
                try {
                    statement.close();
                }
                catch (SQLException sqle) {
                    throw new ConnectException(sqle);
                }
            }
        }
    }
}
