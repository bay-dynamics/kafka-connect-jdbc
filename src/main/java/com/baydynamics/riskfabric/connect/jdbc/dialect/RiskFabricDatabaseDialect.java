package com.baydynamics.riskfabric.connect.jdbc.dialect;

import com.baydynamics.riskfabric.connect.data.EpochMillisConverter;
import com.baydynamics.riskfabric.connect.data.UIDConverter;
import com.baydynamics.riskfabric.connect.jdbc.sink.JdbcBulkWriter;
import com.baydynamics.riskfabric.connect.jdbc.sink.RiskFabricJdbcSinkConfig;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider;
import io.confluent.connect.jdbc.dialect.PostgreSqlDatabaseDialect;
import io.confluent.connect.jdbc.sink.DbStructure;
import io.confluent.connect.jdbc.sink.DbWriter;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.util.*;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.errors.ConnectException;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RiskFabricDatabaseDialect extends PostgreSqlDatabaseDialect {

    protected final JdbcSinkConfig.ColumnCaseType columnCaseType;
    protected final JdbcSinkConfig.CompositeValueBindingMode compositeValueBindingMode;

    public static class Provider extends DatabaseDialectProvider.SubprotocolBasedProvider {
        public Provider() {
            super(RiskFabricDatabaseDialect.class.getSimpleName(), "riskfabricdatabase");
        }

        @Override
        public DatabaseDialect create(AbstractConfig config) {
            return new RiskFabricDatabaseDialect(config);
        }
    }

    protected RiskFabricDatabaseDialect(AbstractConfig config) {
        super(config);

        if (config instanceof RiskFabricJdbcSinkConfig) {
            columnCaseType = ((RiskFabricJdbcSinkConfig) config).columnCaseType;
            compositeValueBindingMode = ((RiskFabricJdbcSinkConfig) config).compositeValueBindingMode;
        } else {
            // not applicable
            columnCaseType = JdbcSinkConfig.ColumnCaseType.valueOf(config.getString(RiskFabricJdbcSinkConfig.DIALECT_RISKFABRIC_TABLE_COLUMNS_CASE_TYPE_DEFAULT).trim().toUpperCase());
            compositeValueBindingMode = JdbcSinkConfig.CompositeValueBindingMode.valueOf(config.getString(RiskFabricJdbcSinkConfig.DIALECT_RISKFABRIC_COMPOSITE_VALUE_BINDING_MODE).trim().toUpperCase());
        }
    }

    @Override
    public int bindField(
        PreparedStatement statement,
        int index,
        Schema schema,
        Object value
    ) throws SQLException {
        if (value == null) {
            statement.setObject(index, null);
        }
        else {
          if (schema.type().equals(Schema.Type.STRUCT) &&
            this.compositeValueBindingMode.equals(JdbcSinkConfig.CompositeValueBindingMode.INDIVIDUAL_COLUMN)) {
            Struct compositeValue = (Struct) value;
            for (final Field nestedField : schema.fields()) {
                bindNonCompositeField(statement, index++, nestedField.schema(), compositeValue.get(nestedField));
            }
          }
          else {
              bindNonCompositeField(statement, index, schema, value);
          }
        }
        return index;
    }

    private void bindNonCompositeField(
        PreparedStatement statement,
        int index,
        Schema schema,
        Object value) throws SQLException {
        boolean bound = maybeBindLogical(statement, index, schema, value);
        if (!bound) {
            bound = maybeBindPrimitive(statement, index, schema, value);
        }
        if (!bound) {
            throw new ConnectException("Unsupported source data type: " + schema.type());
        }
    }

    @Override
    public String buildInsertStatement(
            TableId table,
            FieldsMetadata fieldsMetadata
    ) {
        Collection<ColumnId> keyColumns = asColumnsUsingConfig(table, fieldsMetadata.getKeyFieldsInInsertOrder());
        Collection<ColumnId> nonKeyColumns = asColumnsUsingConfig(table, fieldsMetadata.getNonKeyFieldsInInsertOrder());

        ExpressionBuilder builder = expressionBuilder();
        builder.append("INSERT INTO ");
        builder.append(table);
        builder.append("(");
        builder.appendList()
                .delimitedBy(",")
                .transformedBy((ExpressionBuilder.columnNames()))
                .of(keyColumns, nonKeyColumns);
        builder.append(") VALUES(");
        builder.appendMultiple(",", "?", keyColumns.size() + nonKeyColumns.size());
        builder.append(")");
        return builder.toString();
    }

    @Override
    public String buildUpsertQueryStatement(
        TableId table,
        FieldsMetadata fieldsMetadata
    ) {
        Collection<ColumnId> keyColumns = asColumnsUsingConfig(table, fieldsMetadata.getKeyFieldsInInsertOrder());
        Collection<ColumnId> nonKeyColumns = asColumnsUsingConfig(table, fieldsMetadata.getNonKeyFieldsInInsertOrder());

        ExpressionBuilder builder = expressionBuilder();
        builder.append("INSERT INTO ");
        builder.append(table);
        builder.append(" (");
        builder.appendList()
                .delimitedBy(",")
                .transformedBy((ExpressionBuilder.columnNames()))
                .of(keyColumns, nonKeyColumns);
        builder.append(") VALUES (");
        builder.appendMultiple(",", "?", keyColumns.size() + nonKeyColumns.size());
        builder.append(") ON CONFLICT (");
        builder.appendList()
                .delimitedBy(",")
                .transformedBy(ExpressionBuilder.columnNames())
                .of(keyColumns);
        if (nonKeyColumns.isEmpty()) {
            builder.append(") DO NOTHING");
        } else {
            builder.append(") DO UPDATE SET ");
            builder.appendList()
                .delimitedBy(",")
                .transformedBy(updateClauseTransform)
                .of(nonKeyColumns);
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

    private static ExpressionBuilder.Transform<ColumnId> updateClauseTransform = (builder, col) -> {

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


    // by convention a "." is interpreted as the dot operator to access a composite value property
    // postgres syntax requires to wrap the expression in parenthesis (=EXCLUDED.column_name).field_name
    // only one level supported here, i.e. no nested composite type
    // a dot may occur in a field name if there is a SMT::flatten configured in the connector
    // otherwise dots are not legal within Avro fields names
    public Collection<ColumnId> asColumnsUsingConfig(TableId tableId, Collection<SinkRecordField> fields) {
        return fields.stream()
            .flatMap(field -> {
                if (field.schemaType().equals(Schema.Type.STRUCT)
                        && compositeValueBindingMode.equals(JdbcSinkConfig.CompositeValueBindingMode.INDIVIDUAL_COLUMN)) {

                    return field.schemaFields().stream().map(nestedField ->
                            columnCaseType.equals(JdbcSinkConfig.ColumnCaseType.SNAKE_CASE) ?
                                    new ColumnId(tableId, StringUtils.toSnakeCase(field.name() + "." + nestedField.name()))
                                    : new ColumnId(tableId, nestedField.name()));

                } else {
                    return Stream.of(columnCaseType.equals(JdbcSinkConfig.ColumnCaseType.SNAKE_CASE) ?
                            new ColumnId(tableId, StringUtils.toSnakeCase((field.name())))
                            : new ColumnId(tableId, field.name()));

                }
            })
            .collect(Collectors.toList());
    }
}