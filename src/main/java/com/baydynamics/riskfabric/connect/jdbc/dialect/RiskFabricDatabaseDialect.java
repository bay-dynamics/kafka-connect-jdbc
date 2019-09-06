package com.baydynamics.riskfabric.connect.jdbc.dialect;

import com.baydynamics.riskfabric.connect.data.LocalDateTimeConverter;
import com.baydynamics.riskfabric.connect.data.UIDConverter;
import com.baydynamics.riskfabric.connect.jdbc.sink.RiskFabricJdbcSinkConfig;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider;
import io.confluent.connect.jdbc.dialect.PostgreSqlDatabaseDialect;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.util.*;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.errors.ConnectException;

import javax.ws.rs.NotSupportedException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.stream.Collectors;

public class RiskFabricDatabaseDialect extends PostgreSqlDatabaseDialect {

    protected RiskFabricJdbcSinkConfig.ColumnCaseType columnCaseType = null;
    protected RiskFabricJdbcSinkConfig.CompositeValueBindingMode compositeValueBindingMode = null;

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
            RiskFabricJdbcSinkConfig rfConfig = (RiskFabricJdbcSinkConfig) config;
            columnCaseType = rfConfig.columnCaseType;
            compositeValueBindingMode = rfConfig.compositeValueBindingMode;
        }
        else {
            // not applicable
            columnCaseType = RiskFabricJdbcSinkConfig.ColumnCaseType.valueOf(config.getString(RiskFabricJdbcSinkConfig.DIALECT_RISKFABRIC_TABLE_COLUMNS_CASE_TYPE_DEFAULT).trim().toUpperCase());
            compositeValueBindingMode = RiskFabricJdbcSinkConfig.CompositeValueBindingMode.valueOf(config.getString(RiskFabricJdbcSinkConfig.DIALECT_RISKFABRIC_COMPOSITE_VALUE_BINDING_MODE).trim().toUpperCase());
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
          if (schema.type().equals(Schema.Type.STRUCT)) {
            //Flatten the values for both PROPERTY_AS_COLUMN and ROW_EXPRESSION
            Struct compositeValue = (Struct) value;
            for (final Field nestedField : schema.fields()) {
                bindNonCompositeField(statement, index++, nestedField.schema(), compositeValue.get(nestedField));
            }
            index--; // 1 too far
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
        Collection<SinkRecordField> keyFields = fieldsMetadata.getKeyFieldsInInsertOrder();
        Collection<SinkRecordField> nonKeyFields = fieldsMetadata.getNonKeyFieldsInInsertOrder();

        ExpressionBuilder builder = expressionBuilder();
        builder.append("INSERT INTO ");
        builder.append(table);
        builder.append("(");
        builder.appendList()
                .delimitedBy(",")
                .transformedBy(upsertColumnNameTransform)
                .of(keyFields, nonKeyFields);
        builder.append(") VALUES(");
        builder.appendList()
                .delimitedBy(",")
                .transformedBy(insertValuePlaceHolderTransform)
                .of(keyFields, nonKeyFields);
        builder.append(")");
        return builder.toString();
    }

    public String buildUpdateStatement(
            TableId table,
            FieldsMetadata fieldsMetadata
    ) {
        Collection<SinkRecordField> keyFields = fieldsMetadata.getKeyFieldsInInsertOrder();
        Collection<SinkRecordField> nonKeyFields = fieldsMetadata.getNonKeyFieldsInInsertOrder();

        ExpressionBuilder builder = expressionBuilder();
        builder.append("UPDATE ");
        builder.append(table);
        builder.append(" SET ");
        builder.appendList()
            .delimitedBy(",")
            .transformedBy(updateValueTransform)
            .of(nonKeyFields);
        if (!keyFields.isEmpty()) {
            builder.append(" WHERE ");
            builder.appendList()
                .delimitedBy(" AND ")
                .transformedBy(updateKeyTransform)
                .of(keyFields);
        }
        return builder.toString();
    }

    @Override
    public String buildUpsertQueryStatement(
        TableId table,
        FieldsMetadata fieldsMetadata
    ) {
        Collection<SinkRecordField> keyFields = fieldsMetadata.getKeyFieldsInInsertOrder();
        Collection<SinkRecordField> nonKeyFields = fieldsMetadata.getNonKeyFieldsInInsertOrder();

        ExpressionBuilder builder = expressionBuilder();
        builder.append("INSERT INTO ");
        builder.append(table);
        builder.append(" (");
        builder.appendList()
                .delimitedBy(",")
                .transformedBy(upsertColumnNameTransform)
                .of(keyFields, nonKeyFields);
        builder.append(") VALUES (");
        builder.appendList()
                .delimitedBy(",")
                .transformedBy(insertValuePlaceHolderTransform)
                .of(keyFields, nonKeyFields);
        builder.append(") ON CONFLICT (");
        builder.appendList()
                .delimitedBy(",")
                .transformedBy(upsertColumnNameTransform)
                .of(keyFields);
        if (keyFields.isEmpty()) {
            builder.append(") DO NOTHING");
        } else {
            builder.append(") DO UPDATE SET ");
            builder.appendList()
                .delimitedBy(",")
                .transformedBy(excludeColumnNameTransform)
                .of(nonKeyFields);
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
                case LocalDateTimeConverter.LOGICAL_NAME:
                    statement.setObject(
                        index,
                        LocalDateTimeConverter.toLogical(schema, value),
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

    private ExpressionBuilder.Transform<SinkRecordField> upsertColumnNameTransform = (builder, field) -> {
        if (field.schemaType().equals(Schema.Type.STRUCT)) {
            if (compositeValueBindingMode.equals(RiskFabricJdbcSinkConfig.CompositeValueBindingMode.PROPERTY_AS_COLUMN)) {
                builder
                    .appendList()
                        .delimitedBy(",")
                        .of(field.schemaFields().stream().map(nestedField ->
                            columnCaseType.equals(RiskFabricJdbcSinkConfig.ColumnCaseType.SNAKE_CASE) ?
                                    StringUtils.toSnakeCase(field.name() + "." + nestedField.name())
                                : field.name() + "." + nestedField.name()
                        ).collect(Collectors.toList()));
            }
            else if (compositeValueBindingMode.equals(RiskFabricJdbcSinkConfig.CompositeValueBindingMode.ROW_EXPRESSION)) {
                builder
                    .append(columnCaseType.equals(RiskFabricJdbcSinkConfig.ColumnCaseType.SNAKE_CASE) ?
                        StringUtils.toSnakeCase(field.name())
                        : field.name());
            }
            else {
                throw new NotSupportedException(compositeValueBindingMode.toString());
            }
        }
        else {
            builder
                .append(columnCaseType.equals(RiskFabricJdbcSinkConfig.ColumnCaseType.SNAKE_CASE) ?
                    StringUtils.toSnakeCase(field.name())
                    : field.name()
            );
        }
    };

    private ExpressionBuilder.Transform<SinkRecordField> insertValuePlaceHolderTransform = (builder, field) -> {
        if (field.schemaType().equals(Schema.Type.STRUCT)) {
            if (compositeValueBindingMode.equals(RiskFabricJdbcSinkConfig.CompositeValueBindingMode.PROPERTY_AS_COLUMN)) {
                builder
                    .appendMultiple(",", "?", field.schemaFields().size());
            }
            else if (compositeValueBindingMode.equals(RiskFabricJdbcSinkConfig.CompositeValueBindingMode.ROW_EXPRESSION)) {
                builder
                    .append("ROW(")
                    .appendMultiple(",", "?", field.schemaFields().size())
                    .append(")");
            }
            else {
                throw new NotSupportedException(compositeValueBindingMode.toString());
            }
        }
        else {
            builder
                .append("?");
        }
    };

    private ExpressionBuilder.Transform<SinkRecordField> updateValueTransform = (builder, field) -> this.updateTransform(builder, field, ",");
    private ExpressionBuilder.Transform<SinkRecordField> updateKeyTransform = (builder, field) -> this.updateTransform(builder, field, " AND ");

    private void updateTransform(ExpressionBuilder builder, SinkRecordField field, String delim) {
        if (field.schemaType().equals(Schema.Type.STRUCT)) {
            if (compositeValueBindingMode.equals(RiskFabricJdbcSinkConfig.CompositeValueBindingMode.PROPERTY_AS_COLUMN)) {
                builder
                    .appendList()
                    .delimitedBy(delim)
                    .of(field.schemaFields().stream().map(nestedField -> {
                        String fieldName = field.name();
                        String nestedFieldName = nestedField.name();
                        if (columnCaseType.equals(RiskFabricJdbcSinkConfig.ColumnCaseType.SNAKE_CASE)) {
                            fieldName = StringUtils.toSnakeCase(fieldName);
                            nestedFieldName = StringUtils.toSnakeCase(nestedFieldName);
                        }

                        StringBuilder nestedBuilder = new StringBuilder();
                        nestedBuilder.append(fieldName);
                        nestedBuilder.append(".");
                        nestedBuilder.append(nestedFieldName);
                        nestedBuilder.append("=?");

                        return nestedBuilder.toString();
                    }).collect(Collectors.toList()));
            } else if (compositeValueBindingMode.equals(RiskFabricJdbcSinkConfig.CompositeValueBindingMode.ROW_EXPRESSION)) {
                String fieldName = field.name();
                if (columnCaseType.equals(RiskFabricJdbcSinkConfig.ColumnCaseType.SNAKE_CASE)) {
                    fieldName = StringUtils.toSnakeCase(fieldName);
                }
                builder
                    .append(fieldName)
                    .append("=ROW(")
                    .appendMultiple(",", "?", field.schemaFields().size())
                    .append(")");
            } else {
                throw new NotSupportedException(compositeValueBindingMode.toString());
            }
        } else {
            CharSequence fieldName = columnCaseType.equals(RiskFabricJdbcSinkConfig.ColumnCaseType.SNAKE_CASE) ?
                StringUtils.toSnakeCase(field.name())
                : field.name();
            builder.append(fieldName);
            builder.append("=?");
        }
    };

    private ExpressionBuilder.Transform<SinkRecordField> excludeColumnNameTransform = (builder, field) -> {
        if (field.schemaType().equals(Schema.Type.STRUCT)) {
            if (compositeValueBindingMode.equals(RiskFabricJdbcSinkConfig.CompositeValueBindingMode.PROPERTY_AS_COLUMN)) {
                builder
                    .appendList()
                        .delimitedBy(",")
                        .of(field.schemaFields().stream().map(nestedField -> {
                            String fieldName = field.name();
                            String nestedFieldName = nestedField.name();
                            if (columnCaseType.equals(RiskFabricJdbcSinkConfig.ColumnCaseType.SNAKE_CASE)) {
                                fieldName = StringUtils.toSnakeCase(fieldName);
                                nestedFieldName = StringUtils.toSnakeCase(nestedFieldName);
                            }

                            StringBuilder nestedBuilder = new StringBuilder();
                            nestedBuilder.append(fieldName);
                            nestedBuilder.append(".");
                            nestedBuilder.append(nestedFieldName);
                            nestedBuilder.append("=(EXCLUDED.");
                            nestedBuilder.append(fieldName);
                            nestedBuilder.append(").");
                            nestedBuilder.append(nestedFieldName);

                            return nestedBuilder.toString();
                        }).collect(Collectors.toList()));
            }
            else if (compositeValueBindingMode.equals(RiskFabricJdbcSinkConfig.CompositeValueBindingMode.ROW_EXPRESSION)) {
                String fieldName = field.name();
                if (columnCaseType.equals(RiskFabricJdbcSinkConfig.ColumnCaseType.SNAKE_CASE)) {
                    fieldName = StringUtils.toSnakeCase(fieldName);
                }
                builder
                    .append(fieldName)
                    .append("=EXCLUDED.")
                    .append(fieldName);
            }
            else {
                throw new NotSupportedException(compositeValueBindingMode.toString());
            }
        }
        else
        {
           CharSequence fieldName = columnCaseType.equals(RiskFabricJdbcSinkConfig.ColumnCaseType.SNAKE_CASE) ?
                StringUtils.toSnakeCase(field.name())
                : field.name();

           builder
                .append(fieldName)
                .append("=(EXCLUDED.");
            boolean parenthesisClosed = false;

            for (int i=0;i<fieldName.length();i++) {
                if (fieldName.charAt(i) == '.') {
                    // use case: there may be "." in a raw Avro field name if SMT::flatten STRUCT before the message entered the connector
                    // otherwise "." is illegal in Avro schema field name
                    builder.append(')');
                    builder.append(fieldName.charAt(i));
                    builder.append(fieldName.subSequence(i+1, fieldName.length()));
                    parenthesisClosed = true;
                    break;
                }
                else {
                    builder.append(fieldName.charAt(i));
                }
            }
            if (!parenthesisClosed) {
                builder.append(')');
            }
        }
    };
}