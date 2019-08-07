package io.confluent.connect.jdbc.dialect;

import com.baydynamics.riskfabric.connect.data.EpochMillisConverter;
import com.baydynamics.riskfabric.connect.data.UIDConverter;
import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider;
import io.confluent.connect.jdbc.dialect.PostgreSqlDatabaseDialect;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Schema;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

public class RiskFabricDatabaseDialect extends PostgreSqlDatabaseDialect {
    public RiskFabricDatabaseDialect(AbstractConfig config) {
        super(config);
    }

    public static class Provider extends DatabaseDialectProvider.SubprotocolBasedProvider {
        public Provider() {
            super(RiskFabricDatabaseDialect.class.getSimpleName(), "riskfabricdatabase");
        }

        @Override
        public DatabaseDialect create(AbstractConfig config) {
            return new RiskFabricDatabaseDialect(config);
        }
    }

    @Override
//    public void bindField(
//        PreparedStatement statement,
//        int index,
//        Schema schema,
//        Object value
//    ) throws SQLException {
//        boolean bound = false;
//        if (value != null) {
//            bound = maybeBindConnectName(statement, index, schema, value);
//        }
//        if (!bound) {
//            // Delegate for the remaining logic
//            super.bindField(
//                statement,
//                index,
//                schema,
//                value
//            );
//        }
//    }

//    @Override
//    public int bindCompositeField(
//            PreparedStatement statement,
//            int startIndex,
//            Schema schema,
//            Struct value
//    ) throws SQLException {
//        int lastIndex = startIndex;
//
//        if (schema.name() != null) {
//            switch (schema.name()) {
//                case TimestampConverter.LOGICAL_NAME:
//                    TimestampConverter.toLogical(schema, value);
//
//                    for (final Field nestedField : schema.fields()) {
//                        bindField(statement, lastIndex++, nestedField.schema(), value.get(nestedField));
//                    }
//
//                    break;
//                default:
//            }
//        }
//
//        return super.bindCompositeField(
//            statement,
//            startIndex,
//            schema,
//            value);
//    }

    protected boolean maybeBindLogical(
        PreparedStatement statement,
        int index,
        Schema schema,
        Object value
    ) throws SQLException {
        if (schema.name() != null) {
            switch (schema.name()) {
                case UIDConverter.LOGICAL_NAME:
                    statement.setObject(
                        index,
                        UIDConverter.toLogical(schema, value),
                        java.sql.Types.OTHER
                    );
                    return true;
                case EpochMillisConverter.LOGICAL_NAME:
                    statement.setObject(
                            index,
                            EpochMillisConverter.toLogical(schema, value),
                            Types.TIMESTAMP
                    );
                    return true;

//                case TimestampConverter.LOGICAL_NAME:
//                    statement.setObject(
//                        index,
//                        TimestampConverter.toLogical(schema, value),
//                        java.sql.Types.OTHER
//                    );
//                    return true;
                default:
                    return super.maybeBindLogical(
                        statement,
                        index,
                        schema,
                        value
                    );
            }
        }
        return false;
    }
}
