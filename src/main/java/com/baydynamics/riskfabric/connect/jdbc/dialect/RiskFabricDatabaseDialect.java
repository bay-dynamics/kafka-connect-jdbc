package com.baydynamics.riskfabric.connect.jdbc.dialect;

import com.baydynamics.riskfabric.connect.data.EpochMillisConverter;
import com.baydynamics.riskfabric.connect.data.UIDConverter;
import com.baydynamics.riskfabric.connect.jdbc.sink.JdbcBulkWriter;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider;
import io.confluent.connect.jdbc.dialect.PostgreSqlDatabaseDialect;
import io.confluent.connect.jdbc.sink.DbStructure;
import io.confluent.connect.jdbc.sink.DbWriter;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Schema;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;

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
    public DbWriter getDatabaseWriter() throws UnsupportedOperationException
    {
        if (config instanceof JdbcSinkConfig) {

            JdbcSinkConfig sinkConfig = (JdbcSinkConfig)config;
            if (sinkConfig.insertMode.equals(JdbcSinkConfig.InsertMode.BULKCOPY)) {
                final DbStructure dbStructure = new DbStructure(this);
                return new JdbcBulkWriter(sinkConfig, this, dbStructure);
            }
            else {
                super.getDatabaseWriter();
            }
        }

        throw new UnsupportedOperationException();
    }

    @Override
    public String buildInsertStatement(
            TableId table,
            FieldsMetadata fieldsMetadata
    ) {
        Collection<ColumnId> keyColumns = asColumns(table, fieldsMetadata.keyFieldNames);
        Collection<ColumnId> nonKeyFieldNames = asColumns(table, fieldsMetadata.nonKeyFieldNames);

        ExpressionBuilder builder = expressionBuilder();
        builder.append("INSERT INTO ");
        builder.append(table);
        builder.append("(");
        builder.appendList()
                .delimitedBy(",")
                .transformedBy(ExpressionBuilder.columnNames())
                .of(keyColumns, nonKeyFieldNames);
        builder.append(") VALUES(");
        builder.appendMultiple(",", "?", keyColumns.size() + nonKeyFieldNames.size());
        builder.append(")");
        return builder.toString();
    }

    @Override
    public String buildUpsertQueryStatement(
            TableId table,
            FieldsMetadata fieldsMetadata
    ) {
        Collection<ColumnId> keyColumns = asColumns(table, fieldsMetadata.keyFieldNames);
        Collection<ColumnId> nonKeyFieldNames = asColumns(table, fieldsMetadata.nonKeyFieldNames);

        // by convention a "." in a column name in the RiskFabricDialect means the dot operator to read a composite type field
        // we have to wrap expression in parenthesis (=EXCLUDED.column_name).field_name
        // only one level supported, i.e. no nested composite type
        // a composite name with a "." may happen if it came in as is in SinkRecord
        // this is a separate use case from the Sink Connector flattening the Struct (aka Composite Type) inline in java.
        final ExpressionBuilder.Transform<ColumnId> updateClauseTransform = (builder, col) -> {

            builder.append(col.name());
            builder.append("=(EXCLUDED.");

            boolean parenthesisClosed = false;
            for (int i=0; i<col.name().length();i++) {
                if (col.name().charAt(i) == '.') {
                    builder.append(')');
                    builder.append(col.name().charAt(i));
                    builder.append(col.name().subSequence(i+1, col.name().length()));
                    parenthesisClosed = true;
                    break;
                }
                else {
                    builder.append(col.name().charAt(i));
                }
            }
            if (!parenthesisClosed) {
                builder.append(')');
            }
        };

        ExpressionBuilder builder = expressionBuilder();
        builder.append("INSERT INTO ");
        builder.append(table);
        builder.append(" (");
        builder.appendList()
                .delimitedBy(",")
                .transformedBy(ExpressionBuilder.columnNames())
                .of(keyColumns, nonKeyFieldNames);
        builder.append(") VALUES (");
        builder.appendMultiple(",", "?", keyColumns.size() + nonKeyFieldNames.size());
        builder.append(") ON CONFLICT (");
        builder.appendList()
                .delimitedBy(",")
                .transformedBy(ExpressionBuilder.columnNames())
                .of(keyColumns);
        if (nonKeyFieldNames.isEmpty()) {
            builder.append(") DO NOTHING");
        } else {
            builder.append(") DO UPDATE SET ");
            builder.appendList()
                    .delimitedBy(",")
                    .transformedBy(updateClauseTransform)
                    .of(nonKeyFieldNames);
        }
        return builder.toString();
    }

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
