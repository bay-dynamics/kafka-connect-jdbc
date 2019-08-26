package com.baydynamics.riskfabric.connect.jdbc.sink;

import java.io.StringReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.sink.DbStructure;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import io.confluent.connect.jdbc.util.*;

import com.baydynamics.riskfabric.connect.data.UIDConverter;
import com.baydynamics.riskfabric.connect.jdbc.sink.RiskFabricJdbcSinkConfig;

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
  private ExpressionBuilder copyCommand;
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

      fieldNames = asColumns(tableId, fieldsMetadata.nonKeyFieldNames, config.columnCaseType);

      copyCommand = dbDialect.expressionBuilder();
      copyCommand.append("COPY ");
      copyCommand.append(tableId);
      copyCommand.append(" (");
      copyCommand.appendList()
              .delimitedBy(",")
              .transformedBy(ExpressionBuilder.columnNames())
              .of(fieldNames);
      copyCommand.append(") FROM STDIN CSV ");
    }

    // build CSV row, @TODO use expressionBuilder
    boolean firstColum = true;
    for (final String fieldName : fieldsMetadata.nonKeyFieldNames) {
      if (!firstColum) {
        bufferBuilder.append(COLUMN_DELIMITER);
      }

      int idx = fieldName.indexOf(".");

      if (idx > 0) {
        final String parentFieldName = fieldName.substring(0, idx);
        final String childFieldName = fieldName.substring(idx+1);

        final Field parentField = record.valueSchema().field(parentFieldName);
        if (parentField != null && parentField.schema().type().equals(Schema.Type.STRUCT)) {

          final Field childField = parentField.schema().field(childFieldName);
          if (childField != null && !childField.schema().type().equals(Schema.Type.STRUCT)) {
            Struct parentValue = (Struct)((Struct) record.value()).get(parentField);
            Object childValue = parentValue.get(childFieldName);
            appendColumnValue(bufferBuilder, childField.schema(), childValue);
          }
          else {
            throw new ConnectException(String.format("Invalid field %s. Child field cannot be a STRUCT.", fieldName));
          }

        }
        else {
          String msg = String.format("Invalid field %s. Parent field %s not found.", fieldName, parentFieldName);
          log.error(msg);
          throw new ConnectException(msg);
        }
      }
      else {
        final Field field = record.valueSchema().field(fieldName);
        final Object value = recordValue.get(field);
        final Schema.Type type = field.schema().type();

        appendColumnValue(bufferBuilder, field.schema(), value);
      }

      firstColum = false;
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

  // Helpers

  private Collection<ColumnId> asColumns(TableId tableId, Collection<String> names, RiskFabricJdbcSinkConfig.ColumnCaseType columnCaseType) {
    return names.stream()
      .map(name -> new ColumnId(tableId, name))
      .collect(Collectors.toList());
  }

  private void appendColumnValue(StringBuilder builder, Schema schema, Object value) {
    if (value == null) {
      return;
    }
    else {
      if (schema.name() != null) {
        switch (schema.name()) {
          case UIDConverter.LOGICAL_NAME:
            appendStringQuoted(builder, value);
            return;
          case Decimal.LOGICAL_NAME:
            appendStringQuoted(builder, value);
            return;
          case Date.LOGICAL_NAME:
            appendStringQuoted(builder, DateTimeUtils.formatDate((java.util.Date) value, config.timeZone));
            return;
          case Time.LOGICAL_NAME:
            appendStringQuoted(builder, DateTimeUtils.formatTime((java.util.Date) value, config.timeZone));
            return;
          case org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME:
            appendStringQuoted(builder, DateTimeUtils.formatTimestamp((java.util.Date) value, config.timeZone));
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
          // no escaping required
          appendStringQuoted(builder, value.toString());
          break;
        case BOOLEAN:
          // 1 & 0 for boolean is more portable rather than TRUE/FALSE
          appendStringQuoted(builder, (Boolean) value ? '1' : '0');
          break;
        case STRING:
          appendStringQuoted(builder, value.toString().replaceAll("\"", "\"\""));
          break;
        case BYTES:
          final byte[] bytes;
          if (value instanceof ByteBuffer) {
            final ByteBuffer buffer = ((ByteBuffer) value).slice();
            bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
          } else {
            bytes = (byte[]) value;
          }
          appendStringQuoted(builder, "x'"+ BytesUtil.toHex(bytes) + "'" );
        default:
          throw new ConnectException("Unsupported type for column value: " + schema.type());
      }
    }
  }

  private void appendStringQuoted(StringBuilder builder, Object value) {
    builder.append(QUOTE_CHARACTER + value.toString() + QUOTE_CHARACTER);
  }
}