package com.baydynamics.riskfabric.connect.jdbc.sink;

import com.baydynamics.riskfabric.connect.jdbc.util.CircularFifoHashSet;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.transform.Result;
import java.sql.*;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.concurrent.atomic.AtomicReference;

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
                String msg = String.format("Unable to get or initialize kafka connect sink connector table [%s] for consumer group [%s] topic [%s]", formatTableName(schema, tableName) , consumerGroupId, topicName);
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

    //expects table.rfid to be present (JZ's column)
    //otherwise we could do time based query which would require to specify a datetime column
//    public static CircularFifoHashSet<String> getLatestRecordByInsertOrder(Connection conn, String schema, String tableName, String keyColumnName, String orderByColumn, Integer recordCount) throws ConnectException
//    {
//        PreparedStatement statement = null;
//        try {
//            ExpressionBuilder builder = new ExpressionBuilder();
//            builder.append("SELECT ");
//            builder.append("tail." + keyColumnName);
//            builder.append(" FROM ( ");
//            builder.append( "SELECT ");
//            builder.append(" tbl." + keyColumnName + ", tbl.rfid");
//            builder.append(" FROM " + formatTableName(schema, tableName) + " tbl");
//            builder.append(" WHERE tbl.tenant_uid = get_tenant_uid()"); //TODO should connectors be tenant agnostic? NH: I think so
//            builder.append(" LIMIT " + recordCount);
//            builder.append(") tail");
//            builder.append(" ORDER BY tail." + orderByColumn + " ASC");
//
//            statement = conn.prepareStatement(builder.toString());
//
//            CircularFifoHashSet<String> dedupKeyCache = new CircularFifoHashSet<>(recordCount);
//            ResultSet resultSet = statement.executeQuery();
//            if (resultSet.isBeforeFirst()) {//if state is not empty
//                while (resultSet.next()) {
//                    dedupKeyCache.appendWithNoCheck(resultSet.getString(keyColumnName));
//                }
//                resultSet.close();
//            }
//
//            return dedupKeyCache;
//        }
//        catch (SQLException sqle) {
//            throw new ConnectException(sqle);
//        }
//        finally {
//            if (statement != null) {
//                try {
//                    statement.close();
//                }
//                catch (SQLException sqle) {
//                    throw new ConnectException(sqle);
//                }
//            }
//        }
//    }

    //String[] keyFields, String keyFieldValueSeparator, AtomicReference<CircularFifoHashSet<String>> keyCache) throws ConnectException

    public static void executeQuery(Connection conn, String query, java.util.function.Consumer<ResultSet> reader)
    {
        PreparedStatement statement = null;
        try {
            statement = conn.prepareStatement(query);
            ResultSet resultSet = statement.executeQuery();

            if (resultSet.isBeforeFirst()) {//if state is not empty
                while (resultSet.next()) {
                    reader.accept(resultSet);
                }
                resultSet.close();
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
                String msg = String.format("Unable to get or initialize kafka connect sink connector table [%s] for consumer group [%s] topic [%s]", formatTableName(schema, tableName), consumerGroupId, topicName);
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

    private static String formatTableName(String schema, String tableName) {
        return (schema != null ? schema + "." : "") + tableName;
    }
}