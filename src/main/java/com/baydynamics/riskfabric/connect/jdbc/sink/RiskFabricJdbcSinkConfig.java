package com.baydynamics.riskfabric.connect.jdbc.sink;

import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;
import java.util.Arrays;

public class RiskFabricJdbcSinkConfig extends JdbcSinkConfig {
    public enum ColumnCaseType {
        DEFAULT,
        SNAKE_CASE
    }

    public enum CompositeValueBindingMode {
        ROW_EXPRESSION,
        PROPERTY_AS_COLUMN,
    }

    public enum DeliveryMode {
        AT_LEAST_ONCE,
        EXACTLY_ONCE
    }

    public final CompositeValueBindingMode compositeValueBindingMode;
    public final ColumnCaseType columnCaseType;
    public final DeliveryMode insertDeliveryMode;
    public final int bulkCopyBufferSizeBytes;
    public final int bulkCopyDedupBufferSize;
    public final String bulkCopyDedupBufferQuery;
    public final String bulkCopyDedupBufferKeyFields;

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

    private static final String DIALECT_RISKFABRIC_COMPOSITE_VALUE_BINDING_MODE_DISPLAY = "Semantic for data binding of composite types";

    public static final String DIALECT_RISKFABRIC_TABLE_COLUMNS_CASE_TYPE = "dialect.riskfabric.table.columns.case.type";
    public static final String DIALECT_RISKFABRIC_TABLE_COLUMNS_CASE_TYPE_DEFAULT = "DEFAULT";
    private static final String DIALECT_RISKFABRIC_TABLE_COLUMNS_CASE_TYPE_DOC = "A case type for writing the schema names with the destination column names case type.";
    private static final String DIALECT_RISKFABRIC_TABLE_COLUMNS_CASE_TYPE_DISPLAY = "Case Type";

    public static final String DIALECT_RISKFABRIC_INSERT_BULK_COPY_FILE_SIZE_BYTES = "dialect.riskfabric.insert.bulkcopy.file.size.bytes";
    private static final int DIALECT_RISKFABRIC_INSERT_BULK_COPY_FILE_SIZE_BYTES_DEFAULT = 10000000;
    private static final String DIALECT_RISKFABRIC_INSERT_BULK_COPY_FILE_SIZE_BYTES_DOC =
        "File size of the bulk copy command query in bytes, e.g. ``COPY``.\n"
            + "When file size is reached (with a tolerance to always close last record) the command is executed and a new cmd is started."
            + "This property may subdivide batch.size.";
    private static final String DIALECT_RISKFABRIC_INSERT_BULK_COPY_FILE_SIZE_BYTES_MODE_DISPLAY = "Bulk Copy Command Buffer Size (Bytes)";

    public static final String DIALECT_RISKFABRIC_INSERT_BULK_COPY_DEDUP_BUFFER_SIZE = "dialect.riskfabric.insert.bulkcopy.dedup.buffer.size";
    private static final int DIALECT_RISKFABRIC_INSERT_BULK_COPY_DEDUP_BUFFER_SIZE_DEFAULT = 10000;
    private static final String DIALECT_RISKFABRIC_INSERT_BULK_COPY_DEDUP_BUFFER_SIZE_DOC =
            "Maximum count of record keys to buffer in order to deduplicate a bulk copy batch";
    private static final String DIALECT_RISKFABRIC_INSERT_BULK_COPY_DEDUP_BUFFER_SIZE_DISPLAY = "Bulk Copy deduplication buffer size (count)";

    public static final String DIALECT_RISKFABRIC_INSERT_BULK_COPY_DEDUP_BUFFER_QUERY = "dialect.riskfabric.insert.bulkcopy.dedup.buffer.query";
    private static final String DIALECT_RISKFABRIC_INSERT_BULK_COPY_DEDUP_BUFFER_QUERY_DEFAULT = "";
    private static final String DIALECT_RISKFABRIC_INSERT_BULK_COPY_DEDUP_BUFFER_QUERY_DOC =
            "Query to load record keys from database into memory buffer in order to deduplication";
    private static final String DIALECT_RISKFABRIC_INSERT_BULK_COPY_DEDUP_BUFFER_QUERY_DISPLAY = "Bulk Copy deduplication query";

    public static final String DIALECT_RISKFABRIC_INSERT_BULK_COPY_DEDUP_BUFFER_KEY_FIELDS = "dialect.riskfabric.insert.bulkcopy.dedup.buffer.pk.fields";
    private static final String DIALECT_RISKFABRIC_INSERT_BULK_COPY_DEDUP_BUFFER_KEY_FIELDS_DEFAULT = "";
    private static final String DIALECT_RISKFABRIC_INSERT_BULK_COPY_DEDUP_BUFFER_KEY_FIELDS_DOC =
            "Key fields to extract from the buffer query (to make a key field values are concatenated with `-`";
    private static final String DIALECT_RISKFABRIC_INSERT_BULK_COPY_DEDUP_BUFFER_KEY_FIELDS_DISPLAY = "Bulk Copy deduplication key fields";

    public static final String DIALECT_RISKFABRIC_INSERT_DELIVERY_MODE = "dialect.riskfabric.insert.delivery.mode";
    private static final String DIALECT_RISKFABRIC_INSERT_DELIVERY_MODE_DEFAULT = "at_least_once";
    private static final String DIALECT_RISKFABRIC_INSERT_DELIVERY_MODE_DOC =
            "Delivery mode of the insert command, e.g. ``COPY``.\n"
                    + "``at_least_once``\n"
                    + "``exactly_once``\n";
    private static final String DIALECT_RISKFABRIC_INSERT_DELIVERY_MODE_DISPLAY = "insert delivery mode";

    private static final String RISKFABRIC_GROUP = "Risk Fabric";

    public RiskFabricJdbcSinkConfig(Map<String, String> props) {
        super(props);

        compositeValueBindingMode = CompositeValueBindingMode.valueOf(
            props.getOrDefault(DIALECT_RISKFABRIC_COMPOSITE_VALUE_BINDING_MODE, DIALECT_RISKFABRIC_COMPOSITE_VALUE_BINDING_MODE_DEFAULT)
                .trim()
                .toUpperCase()
        );

        columnCaseType = ColumnCaseType.valueOf(
            props.getOrDefault(DIALECT_RISKFABRIC_TABLE_COLUMNS_CASE_TYPE, DIALECT_RISKFABRIC_TABLE_COLUMNS_CASE_TYPE_DEFAULT)
                .trim()
                .toUpperCase()
        );

        insertDeliveryMode = DeliveryMode.valueOf(
            props.getOrDefault(DIALECT_RISKFABRIC_INSERT_DELIVERY_MODE, DIALECT_RISKFABRIC_INSERT_DELIVERY_MODE_DEFAULT)
                .trim()
                .toUpperCase()
        );

        bulkCopyBufferSizeBytes =
            Integer.valueOf(
                props.getOrDefault(DIALECT_RISKFABRIC_INSERT_BULK_COPY_FILE_SIZE_BYTES,
                Integer.toString(DIALECT_RISKFABRIC_INSERT_BULK_COPY_FILE_SIZE_BYTES_DEFAULT))
        );

        bulkCopyDedupBufferSize =
            Integer.valueOf(
                props.getOrDefault(DIALECT_RISKFABRIC_INSERT_BULK_COPY_DEDUP_BUFFER_SIZE,
                Integer.toString(DIALECT_RISKFABRIC_INSERT_BULK_COPY_DEDUP_BUFFER_SIZE_DEFAULT))
        );

        bulkCopyDedupBufferKeyFields = props.getOrDefault(DIALECT_RISKFABRIC_INSERT_BULK_COPY_DEDUP_BUFFER_KEY_FIELDS,
            DIALECT_RISKFABRIC_INSERT_BULK_COPY_DEDUP_BUFFER_KEY_FIELDS_DEFAULT);

        bulkCopyDedupBufferQuery = props.getOrDefault(DIALECT_RISKFABRIC_INSERT_BULK_COPY_DEDUP_BUFFER_QUERY,
            DIALECT_RISKFABRIC_INSERT_BULK_COPY_DEDUP_BUFFER_QUERY_DEFAULT);
    }

    public static ConfigDef getConfigDef() {
        if (!CONFIG_DEF.configKeys().containsKey(DIALECT_RISKFABRIC_INSERT_DELIVERY_MODE)) {
            CONFIG_DEF
                .define(
                    DIALECT_RISKFABRIC_INSERT_DELIVERY_MODE,
                    ConfigDef.Type.STRING,
                    DIALECT_RISKFABRIC_INSERT_DELIVERY_MODE_DEFAULT,
                    EnumValidator.in(DeliveryMode.values()),
                    ConfigDef.Importance.HIGH,
                    DIALECT_RISKFABRIC_INSERT_DELIVERY_MODE_DOC, RISKFABRIC_GROUP,
                    1,
                    ConfigDef.Width.MEDIUM,
                    DIALECT_RISKFABRIC_INSERT_DELIVERY_MODE_DISPLAY
                );
        }

        if (!CONFIG_DEF.configKeys().containsKey(DIALECT_RISKFABRIC_INSERT_BULK_COPY_FILE_SIZE_BYTES)) {
            CONFIG_DEF
                .define(
                    DIALECT_RISKFABRIC_INSERT_BULK_COPY_FILE_SIZE_BYTES,
                    ConfigDef.Type.INT,
                    DIALECT_RISKFABRIC_INSERT_BULK_COPY_FILE_SIZE_BYTES_DEFAULT,
                    NON_NEGATIVE_INT_VALIDATOR,
                    ConfigDef.Importance.HIGH,
                    DIALECT_RISKFABRIC_INSERT_BULK_COPY_FILE_SIZE_BYTES_DOC, RISKFABRIC_GROUP,
                    2,
                    ConfigDef.Width.MEDIUM,
                    DIALECT_RISKFABRIC_INSERT_BULK_COPY_FILE_SIZE_BYTES_MODE_DISPLAY
                );
        }

        if (!CONFIG_DEF.configKeys().containsKey(DIALECT_RISKFABRIC_COMPOSITE_VALUE_BINDING_MODE)) {
            CONFIG_DEF
                .define(
                    DIALECT_RISKFABRIC_COMPOSITE_VALUE_BINDING_MODE,
                    ConfigDef.Type.STRING,
                    DIALECT_RISKFABRIC_COMPOSITE_VALUE_BINDING_MODE_DEFAULT,
                    EnumValidator.in(CompositeValueBindingMode.values()),
                    ConfigDef.Importance.LOW,
                    DIALECT_RISKFABRIC_COMPOSITE_VALUE_BINDING_MODE_DOC,
                    RISKFABRIC_GROUP,
                    3,
                    ConfigDef.Width.LONG,
                    DIALECT_RISKFABRIC_COMPOSITE_VALUE_BINDING_MODE_DISPLAY
                );
        }

        if (!CONFIG_DEF.configKeys().containsKey(DIALECT_RISKFABRIC_TABLE_COLUMNS_CASE_TYPE)) {
            CONFIG_DEF
                .define(
                    DIALECT_RISKFABRIC_TABLE_COLUMNS_CASE_TYPE,
                    ConfigDef.Type.STRING,
                    DIALECT_RISKFABRIC_TABLE_COLUMNS_CASE_TYPE_DEFAULT,
                    EnumValidator.in(ColumnCaseType.values()),
                    ConfigDef.Importance.LOW,
                    DIALECT_RISKFABRIC_TABLE_COLUMNS_CASE_TYPE_DOC,
                    RISKFABRIC_GROUP,
                    4,
                    ConfigDef.Width.SHORT,
                    DIALECT_RISKFABRIC_TABLE_COLUMNS_CASE_TYPE_DISPLAY
                );
        }

        if (!CONFIG_DEF.configKeys().containsKey(DIALECT_RISKFABRIC_INSERT_BULK_COPY_DEDUP_BUFFER_SIZE)) {
            CONFIG_DEF
                .define(
                    DIALECT_RISKFABRIC_INSERT_BULK_COPY_DEDUP_BUFFER_SIZE,
                    ConfigDef.Type.INT,
                    DIALECT_RISKFABRIC_INSERT_BULK_COPY_DEDUP_BUFFER_SIZE_DEFAULT,
                    NON_NEGATIVE_INT_VALIDATOR,
                    ConfigDef.Importance.MEDIUM,
                    DIALECT_RISKFABRIC_INSERT_BULK_COPY_DEDUP_BUFFER_SIZE_DOC,
                    RISKFABRIC_GROUP,
                    5,
                    ConfigDef.Width.SHORT,
                    DIALECT_RISKFABRIC_INSERT_BULK_COPY_DEDUP_BUFFER_SIZE_DISPLAY
                );
        }

        if (!CONFIG_DEF.configKeys().containsKey(DIALECT_RISKFABRIC_INSERT_BULK_COPY_DEDUP_BUFFER_QUERY)) {
            CONFIG_DEF
                .define(
                    DIALECT_RISKFABRIC_INSERT_BULK_COPY_DEDUP_BUFFER_QUERY,
                    ConfigDef.Type.STRING,
                    DIALECT_RISKFABRIC_INSERT_BULK_COPY_DEDUP_BUFFER_QUERY_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    DIALECT_RISKFABRIC_INSERT_BULK_COPY_DEDUP_BUFFER_QUERY_DOC,
                    RISKFABRIC_GROUP,
                    6,
                    ConfigDef.Width.MEDIUM,
                    DIALECT_RISKFABRIC_INSERT_BULK_COPY_DEDUP_BUFFER_QUERY_DISPLAY
                );
        }

        if (!CONFIG_DEF.configKeys().containsKey(DIALECT_RISKFABRIC_INSERT_BULK_COPY_DEDUP_BUFFER_KEY_FIELDS)) {
            CONFIG_DEF
                .define(
                    DIALECT_RISKFABRIC_INSERT_BULK_COPY_DEDUP_BUFFER_KEY_FIELDS,
                    ConfigDef.Type.STRING,
                    DIALECT_RISKFABRIC_INSERT_BULK_COPY_DEDUP_BUFFER_KEY_FIELDS_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    DIALECT_RISKFABRIC_INSERT_BULK_COPY_DEDUP_BUFFER_KEY_FIELDS_DOC,
                    RISKFABRIC_GROUP,
                    7,
                    ConfigDef.Width.MEDIUM,
                    DIALECT_RISKFABRIC_INSERT_BULK_COPY_DEDUP_BUFFER_KEY_FIELDS_DISPLAY,
                    Arrays.asList(DIALECT_RISKFABRIC_INSERT_BULK_COPY_DEDUP_BUFFER_KEY_FIELDS)
                );
        }

        return CONFIG_DEF;
    }
}