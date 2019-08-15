package com.baydynamics.riskfabric.connect.jdbc.sink;

import java.io.Closeable;
import java.io.StringReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.sink.DbStructure;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.TableId;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Field;

import org.postgresql.core.BaseConnection;
import org.postgresql.copy.CopyManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PgCopyStatement {
  private static final Logger log = LoggerFactory.getLogger(PgCopyStatement.class);

  private final static char COLUMN_DELIMITER = ',';
  private final static char ROW_DELIMITER = '\n';
  private final static char QUOTE_CHARACTER = '"';

  private final TableId tableId;
  private final JdbcSinkConfig config;
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

  private int bufferCapacity = 8000000; //@TODO put in config


  public PgCopyStatement(
          JdbcSinkConfig config,
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

    // build CSV row for record
    for (final String fieldName : fieldsMetadata.nonKeyFieldNames) {
      final Field field = record.valueSchema().field(fieldName);
      Struct valueStruct = (Struct) record.value();
      Object fieldValue = valueStruct.get(field);

      if (fieldValue == null) {
        bufferBuilder.append(COLUMN_DELIMITER);
      }
      else {
        bufferBuilder.append(fieldValue.toString()); //@TODO HOW format string representation + escape quotes
        bufferBuilder.append(COLUMN_DELIMITER);
      }
    }
    bufferBuilder.append(ROW_DELIMITER);
    bufferWatermark=bufferBuilder.length();

    records.add(record);

    if (bufferBuilder.length() > bufferCapacity) {
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

    records = new ArrayList<>();
    return flushedRecords;
  }

  public void close() throws SQLException {
    flush();
  }

  public Collection<ColumnId> asColumns(TableId tableId, Collection<String> names, JdbcSinkConfig.ColumnCaseType columnCaseType) {
    return names.stream()
        .map(name -> new ColumnId(tableId, name, columnCaseType))
        .collect(Collectors.toList());
  }

}
