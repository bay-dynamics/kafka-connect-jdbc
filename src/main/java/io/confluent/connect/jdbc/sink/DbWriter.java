package io.confluent.connect.jdbc.sink;

import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public interface DbWriter {
    void write(final Collection<SinkRecord> records) throws SQLException;
    void closeQuietly();
    TableId destinationTable(String topic);
}
