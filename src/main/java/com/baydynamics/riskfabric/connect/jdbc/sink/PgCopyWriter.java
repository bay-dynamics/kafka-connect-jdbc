package com.baydynamics.riskfabric.connect.jdbc.sink;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.sink.DbStructure;
import io.confluent.connect.jdbc.sink.GenericDbWriter;
import io.confluent.connect.jdbc.util.TableId;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;

public class PgCopyWriter extends GenericDbWriter {
    private static final Logger log = LoggerFactory.getLogger(PgCopyWriter.class);

    public PgCopyWriter(final RiskFabricJdbcSinkConfig config, DatabaseDialect dbDialect, DbStructure dbStructure) throws ConnectException {
        super(config, dbDialect, dbStructure);
    }

    public void write(final Collection<SinkRecord> records) throws SQLException {
        final Connection connection = cachedConnectionProvider.getConnection();
        PgCopy copyStatement = null;
        for (SinkRecord record : records) {
            if (copyStatement == null) {
                final TableId tableId = destinationTable(record.topic());
                copyStatement = new PgCopy((RiskFabricJdbcSinkConfig)config, tableId, dbDialect, dbStructure, connection);
            }
            copyStatement.add(record);
        }

        copyStatement.close();

        connection.commit();
    }
}