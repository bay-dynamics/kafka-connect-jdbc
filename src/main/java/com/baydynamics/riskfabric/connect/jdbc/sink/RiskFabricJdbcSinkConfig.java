package com.baydynamics.riskfabric.connect.jdbc.sink;

import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class RiskFabricJdbcSinkConfig extends JdbcSinkConfig {

    public enum ColumnCaseType {
        DEFAULT,
        SNAKE_CASE
    }

    public enum CompositeValueBindingMode {
        ROW_EXPRESSION,
        PROPERTY_AS_COLUMN,
    }

    public final CompositeValueBindingMode compositeValueBindingMode;
    public final ColumnCaseType columnCaseType;

    public static final String DIALECT_RISKFABRIC_COMPOSITE_VALUE_BINDING_MODE = "dialect.riskfabric.composite.value.binding.mode";
    private static final String DIALECT_RISKFABRIC_COMPOSITE_VALUE_BINDING_MODE_DEFAULT = "row_expression";
    private static final String DIALECT_RISKFABRIC_COMPOSITE_VALUE_BINDING_MODE_DOC =
            "For RiskFabricDatabaseDialect, the binding mode to use for composite values (fields whose value is a STRUCT): \n"
                    + "``property_as_column``\n"
                    + "   fields are flattened and mapped to the composite destination column using a `.` semantic if it is supported by the connector, e.g.\n"
                    + "   INSERT INTO table(compositeColumn.property1, compositeColumn.compositeTypeProperty2) VALUES (<struct.field1.value>, <struct.field2.value>) statements.\n"
                    + "``row_expression``\n"
                    + "   Use a ``row_expression`` semantic to write the composite value as single value in the destination column if it is supported by the connector, e.g.\n"
                    + "   INSERT INTO table(compositeColumn) VALUES (ROW(<struct.field1.value, <struct.field2.value>)).\n";

    private static final String DIALECT_RISKFABRIC_COMPOSITE_VALUE_BINDING_MODE_DISPLAY = "Bulk Copy Command Buffer Size (Bytes)";

    public static final String DIALECT_RISKFABRIC_TABLE_COLUMNS_CASE_TYPE = "dialect.riskfabric.table.columns.case.type";
    public static final String DIALECT_RISKFABRIC_TABLE_COLUMNS_CASE_TYPE_DEFAULT = "DEFAULT";
    private static final String DIALECT_RISKFABRIC_TABLE_COLUMNS_CASE_TYPE_DOC = "A case type for writing the schema names with the destination column names case type.";
    private static final String DIALECT_RISKFABRIC_TABLE_COLUMNS_CASE_TYPE_DISPLAY = "Case Type";

    private static final String RISKFABRIC_GROUP = "Risk Fabric";

    public RiskFabricJdbcSinkConfig(Map<String, String> props) {
        super(props);

        CONFIG_DEF
            .define(
                DIALECT_RISKFABRIC_COMPOSITE_VALUE_BINDING_MODE,
                ConfigDef.Type.STRING,
                DIALECT_RISKFABRIC_COMPOSITE_VALUE_BINDING_MODE_DEFAULT,
                EnumValidator.in(CompositeValueBindingMode.values()),
                ConfigDef.Importance.LOW,
                DIALECT_RISKFABRIC_COMPOSITE_VALUE_BINDING_MODE_DOC,
                RISKFABRIC_GROUP,
                1,
                ConfigDef.Width.LONG,
                DIALECT_RISKFABRIC_COMPOSITE_VALUE_BINDING_MODE_DISPLAY
            )
            .define(
                DIALECT_RISKFABRIC_TABLE_COLUMNS_CASE_TYPE,
                ConfigDef.Type.STRING,
                DIALECT_RISKFABRIC_TABLE_COLUMNS_CASE_TYPE_DEFAULT,
                EnumValidator.in(ColumnCaseType.values()),
                ConfigDef.Importance.LOW,
                DIALECT_RISKFABRIC_TABLE_COLUMNS_CASE_TYPE_DOC,
                RISKFABRIC_GROUP,
                2,
                ConfigDef.Width.SHORT,
                DIALECT_RISKFABRIC_TABLE_COLUMNS_CASE_TYPE_DISPLAY
            );

        compositeValueBindingMode = CompositeValueBindingMode.valueOf(
            ((String)props.getOrDefault(DIALECT_RISKFABRIC_COMPOSITE_VALUE_BINDING_MODE, DIALECT_RISKFABRIC_COMPOSITE_VALUE_BINDING_MODE_DEFAULT))
                .trim()
                .toUpperCase()
        );

        columnCaseType = ColumnCaseType.valueOf(
            ((String)props.getOrDefault(DIALECT_RISKFABRIC_TABLE_COLUMNS_CASE_TYPE, DIALECT_RISKFABRIC_TABLE_COLUMNS_CASE_TYPE_DEFAULT))
                .trim()
                .toUpperCase()
        );
    }
}
