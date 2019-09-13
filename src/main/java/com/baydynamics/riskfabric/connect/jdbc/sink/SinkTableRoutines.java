package com.baydynamics.riskfabric.connect.jdbc.sink;

import org.apache.kafka.connect.errors.ConnectException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;

// NH: SinkTableState is a crappy name/model, I expect it to change.
// Tracking offset per table per topic per partition will most likely move into other kafka administrative tables that are being developed in other projects.
public class SinkTableRoutines {
    private static final Logger log = LoggerFactory.getLogger(SinkTableRoutines.class);

    public static Iterable<SinkTableState> getOrInitializeTopicPartitionState(Connection conn, String schema, String tableName, String topicName, Integer[] partitionIds) throws ConnectException
    {
        PreparedStatement statement = null;
        try {
            statement = conn.prepareStatement("SELECT * FROM get_or_initialize_kafka_connect_sink_table_state(?,?,?,?)");
            statement.setString(1, topicName);
            statement.setArray(2, conn.createArrayOf("integer", partitionIds));
            statement.setString(3, tableName);
            statement.setString(4, schema);

            ArrayList<SinkTableState> state = new ArrayList<SinkTableState>();

            ResultSet rs = statement.executeQuery();
            if (rs.isBeforeFirst()) {//if state is not empty
                while (rs.next()) {
                    state.add(new SinkTableState(){{
                        setSchemaName(rs.getString("schema_name"));
                        setTableName(rs.getString("table_name"));
                        setKafkaTopic(rs.getString("kafka_topic"));
                        setKafkaPartition(rs.getInt("kafka_partition"));
                        setTableKafkaOffset(rs.getLong("table_kafka_offset"));
                    }});
                }
                rs.close();

                return state;
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

    public static void updateTopicPartitionState(Connection conn, String schema, String tableName, String topicName, Integer[] partitionIds, Long[] partitionOffsets) throws ConnectException
    {
        PreparedStatement statement = null;
        try {
            statement = conn.prepareStatement("SELECT * FROM update_kafka_connect_sink_table_state(?,?,?,?,?)");
            statement.setString(1, topicName);
            statement.setArray(2, conn.createArrayOf("integer", partitionIds));
            statement.setArray(3, conn.createArrayOf("bigint", partitionOffsets));
            statement.setString(4, tableName);
            statement.setString(5, schema);

            ResultSet rs = statement.executeQuery();
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