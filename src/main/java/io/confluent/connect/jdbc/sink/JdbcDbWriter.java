/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.sink;

import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcDbWriter extends GenericDbWriter {
  private static final Logger log = LoggerFactory.getLogger(JdbcDbWriter.class);

  public JdbcDbWriter(final JdbcSinkConfig config, DatabaseDialect dbDialect, DbStructure dbStructure){
    super(config, dbDialect, dbStructure);
  }

  public void write(final Collection<SinkRecord> records) throws SQLException {
    final Connection connection = cachedConnectionProvider.getConnection();

    final Map<TableId, BufferedRecords> bufferByTable = new HashMap<>();
    for (SinkRecord record : records) {
      final TableId tableId = destinationTable(record.topic());
      BufferedRecords buffer = bufferByTable.get(tableId);
      if (buffer == null) {
        buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, connection);
        bufferByTable.put(tableId, buffer);
      }
      buffer.add(record);
    }
    for (Map.Entry<TableId, BufferedRecords> entry : bufferByTable.entrySet()) {
      TableId tableId = entry.getKey();
      BufferedRecords buffer = entry.getValue();
      log.debug("Flushing records in JDBC Writer for table ID: {}", tableId);
      buffer.flush();
      buffer.close();
    }
    connection.commit();
  }
}