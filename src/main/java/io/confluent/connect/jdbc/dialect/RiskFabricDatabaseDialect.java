package io.confluent.connect.jdbc.dialect;

import com.baydynamics.riskfabric.connect.data.UID;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Schema;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class RiskFabricDatabaseDialect extends PostgreSqlDatabaseDialect {
    /**
     * Create a new dialect instance with the given connector configuration.
     *
     * @param config the connector configuration; may not be null
     */
    public RiskFabricDatabaseDialect(AbstractConfig config) {
        super(config);
    }


    @Override
    public void bindField(
            PreparedStatement statement,
            int index,
            Schema schema,
            Object value
    ) throws SQLException {
        boolean bound = false;
        if (value != null) {
            bound = maybeBindConnectName(statement, index, schema, value);
        }
        if (!bound) {
            // Delegate for the remaining logic
            super.bindField(
                    statement,
                    index,
                    schema,
                    value
            );
        }
    }

    protected boolean maybeBindConnectName(
            PreparedStatement statement,
            int index,
            Schema schema,
            Object value
    ) throws SQLException {
        if (schema.name() != null) {
            switch (schema.name()) {
                case UID.LOGICAL_NAME:
                    statement.setObject(
                            index,
                            UID.toLogical(schema, value.toString()),
                            java.sql.Types.OTHER
                    );
                    return true;
                default:
                    return false;
            }
        }
        return false;
    }

    /**
     * The provider for {@link PostgreSqlDatabaseDialect}.
     */
    public static class Provider extends DatabaseDialectProvider.SubprotocolBasedProvider {
        public Provider() {
            super(RiskFabricDatabaseDialect.class.getSimpleName(), "riskfabricdatabase");
        }

        @Override
        public DatabaseDialect create(AbstractConfig config) {
            return new RiskFabricDatabaseDialect(config);
        }
    }
}