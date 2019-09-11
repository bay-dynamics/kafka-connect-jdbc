package com.baydynamics.riskfabric.connect.jdbc.sink;

import org.apache.kafka.connect.errors.ConnectException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.UUID;

//NH: SinkTableState is crappy name/model, I expect it will change
// tracking offset per table per topic per partition will most likely fall into other kafka administrative table that we are developing in other projects
public class SinkTableRoutines {
    private static final Logger log = LoggerFactory.getLogger(SinkTableRoutines.class);

    public static Iterable<SinkTableState> getOrInitializeTopicPartitionState(Connection conn, String schema, String tableName, String topicName, Object[] partitionIds) throws ConnectException
    {
        PreparedStatement statement = null;
        try {
            statement = conn.prepareStatement("SELECT * FROM get_or_initialize_kafka_connect_sink_table_state(?, ?, ?, ?)");
            statement.setString(1, schema);
            statement.setString(2, tableName);
            statement.setString(3, topicName);
            statement.setArray(4, conn.createArrayOf("integer", partitionIds));

            ArrayList state = new ArrayList<SinkTableState>();

            ResultSet rs = statement.executeQuery();
            if (rs.isBeforeFirst()) {//if state is not empty
                while (rs.next()) {
                    state.add(new SinkTableState(){{
                        setUid(UUID.fromString(rs.getString("kafka_connect_sink_table_state_uid")));
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

    public static void updateTopicPartitionState(Connection conn, Object[] partitionStateUids, Object[] partitionOffset) // throws ConnectException
    {
        PreparedStatement statement = null;
        try {
            statement = conn.prepareStatement("SELECT * FROM update_kafka_connect_sink_table_state(?, ?");
            statement.setArray(4, conn.createArrayOf("uuid", partitionStateUids));
            statement.setArray(4, conn.createArrayOf("bigint", partitionOffset));
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
