package io.confluent.connect.jdbc.sink;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public abstract class GenericDbWriter implements DbWriter {
    private static final Logger log = LoggerFactory.getLogger(GenericDbWriter.class);

    protected final JdbcSinkConfig config;
    protected final DatabaseDialect dbDialect;
    protected final DbStructure dbStructure;
    protected final CachedConnectionProvider cachedConnectionProvider;

    public GenericDbWriter(final JdbcSinkConfig config, DatabaseDialect dbDialect, DbStructure dbStructure) {
        this.config = config;
        this.dbDialect = dbDialect;
        this.dbStructure = dbStructure;

        this.cachedConnectionProvider = new CachedConnectionProvider(this.dbDialect) {
            @Override
            protected void onConnect(Connection connection) throws SQLException {
                log.info("GenericDbWriter Connected");
                connection.setAutoCommit(false);
            }
        };
    }

    public abstract void write(final Collection<SinkRecord> records) throws SQLException;

    public void closeQuietly() {
        cachedConnectionProvider.close();
    }

    public TableId destinationTable(String topic) {
        final String tableName = config.tableNameFormat.replace("${topic}", topic);
        if (tableName.isEmpty()) {
            throw new ConnectException(String.format(
                    "Destination table name for topic '%s' is empty using the format string '%s'",
                    topic,
                    config.tableNameFormat
            ));
        }
        return dbDialect.parseTableIdentifier(tableName);
    }
}
