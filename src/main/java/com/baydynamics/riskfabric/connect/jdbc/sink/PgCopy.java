package com.baydynamics.riskfabric.connect.jdbc.sink;

import java.io.StringReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.sink.DbStructure;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import io.confluent.connect.jdbc.util.*;

import com.baydynamics.riskfabric.connect.data.UIDConverter;
import com.baydynamics.riskfabric.connect.data.LocalDateTimeConverter;

import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import org.postgresql.core.BaseConnection;
import org.postgresql.copy.CopyManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PgCopy {
  private static final Logger log = LoggerFactory.getLogger(PgCopy.class);

  private final static char COLUMN_DELIMITER = ',';
  private final static char ROW_DELIMITER = '\n';
  private final static char QUOTE_CHARACTER = '"';
  private final static char ESCAPE_CHARACTER = '"';

  private final TableId tableId;
  private final RiskFabricJdbcSinkConfig config;
  private final DatabaseDialect dbDialect;
  private final DbStructure dbStructure;
  private final Connection connection;

  private List<SinkRecord> records = new ArrayList<>();

  Schema keySchema;
  Schema valueSchema;
  FieldsMetadata fieldsMetadata;
  Collection<ColumnId> fieldNames;

  private final CopyManager copyManager;
  private StringBuilder copyCommand;
  private StringBuilder bufferBuilder = null;
  private int bufferWatermark;
  private Struct parentValue;

  public PgCopy(
      RiskFabricJdbcSinkConfig config,
      TableId tableId,
      DatabaseDialect dbDialect,
      DbStructure dbStructure,
      Connection connection) throws SQLException {

    this.tableId = tableId;
    this.config = config;
    this.dbDialect = dbDialect;
    this.dbStructure = dbStructure;
    this.connection = connection;
    this.copyManager = new CopyManager((BaseConnection) connection);
    this.bufferBuilder = new StringBuilder();
    this.bufferWatermark = 0;
  }

  public List<SinkRecord> add(SinkRecord record) throws SQLException {
    final List<SinkRecord> flushed = new ArrayList<>();

    if (!record.valueSchema().type().equals(Schema.Type.STRUCT)) {
      throw new ConnectException("record must be of type STRUCT");
    }

    Struct recordValue = (Struct) record.value();

    boolean schemaChanged = false;
    if (!Objects.equals(keySchema, record.keySchema())) {
      keySchema = record.keySchema();
      schemaChanged = true;
    }

    if (Objects.equals(valueSchema, record.valueSchema())) {
      valueSchema = record.valueSchema();
      schemaChanged = true;
    }

    if (schemaChanged) {
      // Each batch needs to have the same schemas, so get the buffered records out
      flushed.addAll(flush());

      // re-initialize everything that depends on the record schema
      final SchemaPair schemaPair = new SchemaPair(
        record.keySchema(),
        record.valueSchema()
      );
      fieldsMetadata = FieldsMetadata.extract(
        tableId.tableName(),
        config.pkMode,
        config.pkFields,
        config.fieldsWhitelist,
        schemaPair
      );

      boolean firstColumn = true;
      copyCommand = new StringBuilder();
      copyCommand.append("COPY ");
      copyCommand.append(tableId.schemaName() + "." + tableId.tableName());
      copyCommand.append(" (");
      for (String fieldName : fieldsMetadata.nonKeyFieldNames) {
        if (!firstColumn) {
          copyCommand.append(COLUMN_DELIMITER);
        }
        else {
          firstColumn = false;
        }

        if (config.columnCaseType == RiskFabricJdbcSinkConfig.ColumnCaseType.SNAKE_CASE) {
          copyCommand.append(StringUtils.toSnakeCase(fieldName));
        }
        else {
          copyCommand.append(fieldName);
        }
      }
      copyCommand.append(") FROM STDIN WITH ");
      copyCommand.append(" CSV ");
      copyCommand.append(" QUOTE AS '" + QUOTE_CHARACTER + "' ");
      copyCommand.append(" ESCAPE AS '" + ESCAPE_CHARACTER + "' ");
    }

    boolean firstColumn = true;
    for (final String fieldName : fieldsMetadata.nonKeyFieldNames) {
      if (!firstColumn) {
        bufferBuilder.append(COLUMN_DELIMITER);
      }
      else {
        firstColumn = false;
      }

      final Field field = record.valueSchema().field(fieldName);
      final Object value = recordValue.get(field);
      appendFieldValue(bufferBuilder, field.schema(), value, QUOTE_CHARACTER);
    }
    bufferBuilder.append(ROW_DELIMITER);
    bufferWatermark=bufferBuilder.length();

    records.add(record);

    if (records.size() >= config.batchSize || bufferBuilder.length() >= config.bulkCopyBufferSizeBytes) {
      flush();
    }

    return flushed;
  }

  public List<SinkRecord> flush() throws SQLException {
    if (records.isEmpty()) {
      log.debug("Records is empty");
      return new ArrayList<>();
    }

    if (bufferWatermark > 0) {
      try {
        copyManager.copyIn(copyCommand.toString(), new StringReader(bufferBuilder.substring(0, bufferWatermark).toString()));
        bufferBuilder.delete(0, bufferWatermark);//remove written rows from the buffer
        bufferWatermark = 0;
      }
      catch (IOException exception) {
        throw new SQLException(exception.getMessage());
      }
    }

    final List<SinkRecord> flushedRecords = records;

    this.records = new ArrayList<>();
    return flushedRecords;
  }

  public void close() throws SQLException {
    flush();
  }

  private void appendFieldValue(StringBuilder builder, Schema schema , Object value, char quoteCharacter) {
    if (value == null) {
      return;
    }
    else {
        if (schema.name() != null) {
            switch (schema.name()) {
                case UIDConverter.LOGICAL_NAME:
                    appendValueQuoted(builder, (String)value, quoteCharacter);
                    return;
                case LocalDateTimeConverter.LOGICAL_NAME:
                    appendValueQuoted(builder, LocalDateTimeConverter.toLogical(schema, value).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME), quoteCharacter);
                    return;
                case Decimal.LOGICAL_NAME:
                    builder.append(value);
                    return;
                case Date.LOGICAL_NAME:
                    appendValueQuoted(builder, DateTimeUtils.formatDate((java.util.Date) value, config.timeZone), quoteCharacter);
                    return;
                case Time.LOGICAL_NAME:
                    appendValueQuoted(builder, DateTimeUtils.formatTime((java.util.Date) value, config.timeZone), quoteCharacter);
                    return;
                case org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME:
                    appendValueQuoted(builder, DateTimeUtils.formatTimestamp((java.util.Date) value, config.timeZone), quoteCharacter);
                    return;
                default:
                // fall through to regular types
                break;
            }
        }

        switch (schema.type()) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case FLOAT32:
            case FLOAT64:
              bufferBuilder.append(value.toString());
              break;
            case BOOLEAN:
              // 1 & 0 for boolean is more portable rather than TRUE/FALSE
              bufferBuilder.append((Boolean) value ? quoteCharacter + 1 + quoteCharacter : quoteCharacter + 0 + quoteCharacter);
              break;
            case STRING:
              appendValueEscaped(builder, (String)value, quoteCharacter);
              break;
            case BYTES:
              final byte[] bytes;
              if (value instanceof ByteBuffer) {
                final ByteBuffer buffer = ((ByteBuffer) value).slice();
                bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
              }
              else {
                bytes = (byte[]) value;
              }
              bufferBuilder.append("x'"+ BytesUtil.toHex(bytes) + "'");
              break;
            case STRUCT:
              final Struct compositeValue = (Struct)value;
              if (config.compositeValueBindingMode == RiskFabricJdbcSinkConfig.CompositeValueBindingMode.ROW_EXPRESSION) {
                appendRowExpressionValue(builder, compositeValue, quoteCharacter,'\'');
              }
              else {
                String msg = String.format("binding.mode=%s is not supported.", config.compositeValueBindingMode.toString());
                log.error(msg);
                throw new ConnectException(msg);
              }
              break;
            default:
              throw new ConnectException("Unsupported type for column value: " + schema.type());
        }
    }
  }

  private void appendRowExpressionValue(StringBuilder builder, Struct compositeValue, char quoteCharacter, char compositeValueQuoteCharacter) {
    bufferBuilder.append(quoteCharacter);
    // interestingly you have to omit ROW here otherwise it is not recognized by COPY
    bufferBuilder.append("(");
    boolean firstField = true;
    for (final Field compositeField : compositeValue.schema().fields()) {
      if (!firstField) {
        bufferBuilder.append(COLUMN_DELIMITER);
      }
      else {
        firstField = false;
      }
      Object compositeFieldValue = compositeValue.get(compositeField);
      appendFieldValue(builder, compositeField.schema(), compositeFieldValue, compositeValueQuoteCharacter);
    }
    bufferBuilder.append(')');
    bufferBuilder.append(quoteCharacter);
  }

  private void appendValueEscaped(StringBuilder builder, String value, char quoteCharacter) {
    builder.append(quoteCharacter);
    for (int i=0;i<value.length();i++) {
      if (value.charAt(i) == ESCAPE_CHARACTER) {
        bufferBuilder.append(ESCAPE_CHARACTER);
        bufferBuilder.append(ESCAPE_CHARACTER);
      } else if (value.charAt(i) == quoteCharacter) {
        bufferBuilder.append(ESCAPE_CHARACTER);
        bufferBuilder.append(quoteCharacter);
      } else {
        bufferBuilder.append(value.charAt(i));
      }
    }
    builder.append(quoteCharacter);
  }

  private void appendValueQuoted(StringBuilder builder, String value, char quoteCharacter) {
    builder.append(quoteCharacter);
    builder.append(value);
    builder.append(quoteCharacter);
  }
}