package com.baydynamics.riskfabric.connect.jdbc.sink;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;

// NH: SinkTableState is a crappy name/model, I expect it to change.
// Tracking offset per table per topic per partition will most likely move into other kafka administrative tables that are being developed in other projects.
public class PgRoutines {
    private static final Logger log = LoggerFactory.getLogger(PgRoutines.class);

    public static HashMap<TopicPartition, Long> getOrInitializeTopicPartitions(Connection conn, String consumerGroupId, String schema, String tableName, String topicName, Integer[] partitionIds) throws ConnectException
    {
        PreparedStatement statement = null;
        try {
            statement = conn.prepareStatement("SELECT * FROM get_or_initialize_kafka_connect_sink_connector_mapping(?,?,?,?,?)");
            statement.setString(1, consumerGroupId);
            statement.setString(2, topicName);
            statement.setArray(3, conn.createArrayOf("integer", partitionIds));
            statement.setString(4, tableName);
            statement.setString(5, schema);

            ResultSet rs = statement.executeQuery();
            if (rs.isBeforeFirst()) {//if state is not empty
                HashMap<TopicPartition, Long> dbOffsetMap = toOffsetMap(rs);
                rs.close();

                return dbOffsetMap;
            }
            else
            {
                String msg = String.format("Unable to get or initialize kafka connect sink connector table [%s] for consumer group [%s] topic [%s]", (schema != null ? schema + "." : "") + tableName , consumerGroupId, topicName);
                log.error("get_or_initialize_kafka_connect_sink_connector_topic_partition returned nothing.");
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

    public static HashMap<TopicPartition, Long> updateWatermarkOffsets(Connection conn, String consumerGroupId, String schema, String tableName, String topicName, Integer[] partitionIds, Long[] partitionOffsets) throws ConnectException
    {
        PreparedStatement statement = null;
        try {
            statement = conn.prepareStatement("SELECT * FROM update_kafka_connect_sink_connector_table_watermark_offset(?,?,?,?,?,?)");
            statement.setString(1, consumerGroupId);
            statement.setString(2, topicName);
            statement.setArray(3, conn.createArrayOf("integer", partitionIds));
            statement.setArray(4, conn.createArrayOf("bigint", partitionOffsets));
            statement.setString(5, tableName);
            statement.setString(6, schema);

            ResultSet rs = statement.executeQuery();
            if (rs.isBeforeFirst()) {//if state is not empty
                HashMap<TopicPartition, Long> dbOffsetMap = toOffsetMap(rs);
                rs.close();
                return dbOffsetMap;
            }
            else
            {
                String msg = String.format("Unable to get or initialize kafka connect sink connector table [%s] for consumer group [%s] topic [%s]", (schema != null ? schema + "." : "") + tableName , consumerGroupId, topicName);
                log.error("get_or_initialize_kafka_connect_sink_connector_topic_partition returned nothing.");
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

    private static HashMap<TopicPartition, Long> toOffsetMap(ResultSet rs) throws SQLException {
        HashMap<TopicPartition, Long> dbOffsetMap = new HashMap<TopicPartition, Long>();
        while (rs.next()) {
            dbOffsetMap.put(
                    new TopicPartition(
                            rs.getString("kafka_topic"),
                            rs.getInt("kafka_partition")
                    ), rs.getLong("table_watermark_offset")
            );
        }
        return dbOffsetMap;
    }
}