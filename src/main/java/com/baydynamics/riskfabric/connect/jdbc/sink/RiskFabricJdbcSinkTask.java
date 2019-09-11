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

package com.baydynamics.riskfabric.connect.jdbc.sink;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialects;
import io.confluent.connect.jdbc.sink.DbStructure;
import io.confluent.connect.jdbc.sink.DbWriter;
import io.confluent.connect.jdbc.sink.JdbcDbWriter;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;

import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;

public class RiskFabricJdbcSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(RiskFabricJdbcSinkTask.class);

  DatabaseDialect dialect;
  RiskFabricJdbcSinkConfig config;
  DbWriter writer;
  int remainingRetries;

  SinkTaskContext sinkTaskContext;
  Collection<TopicPartition> topicPartitions;

  /**
   * Initialise sink task
   * @param context context of the sink task
   */
  @Override
  public void initialize(SinkTaskContext context) {
    sinkTaskContext=context;//save task context
  }

  @Override
  public void start(final Map<String, String> props) throws ConnectException {
    log.info("Starting Risk Fabric JDBC Sink task");
    config = new RiskFabricJdbcSinkConfig(props);
    remainingRetries = config.maxRetries;
    writer = createWriter();

    if (writer instanceof PgCopyWriterSynchronized) {
      HashMap<TopicPartition,Long> offsetMaps = ((PgCopyWriterSynchronized) writer).getOffsetMaps();
      sinkTaskContext.offset(offsetMaps);//synchronise offsets
    }
  }

  private DbWriter createWriter() {
    dialect = DatabaseDialects.create("RiskFabricDatabaseDialect", config);
    log.info("Initializing writer for insert.mode {} using SQL dialect: {}", config.insertMode, dialect.getClass().getSimpleName());
    final DbStructure dbStructure = new DbStructure(dialect);

    if (config.insertMode.equals(JdbcSinkConfig.InsertMode.BULKCOPY)) {
      if (config.bulkCopyDeliveryMode == JdbcSinkConfig.DeliveryMode.SYNCHRONIZED) {
        return new PgCopyWriterSynchronized(config, dialect, dbStructure, topicPartitions);
      }
      else {
        return new PgCopyWriter(config, dialect, dbStructure);
      }
    }
    else {
        return new JdbcDbWriter(config, dialect, dbStructure);
    }
  }

  public void open(Collection<TopicPartition> partitions) {
      // save for later
      topicPartitions = partitions;
      super.open(partitions);
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    if (records.isEmpty()) {
      return;
    }
    final SinkRecord first = records.iterator().next();
    final int recordsCount = records.size();
    log.debug(
        "Received {} records. First record kafka coordinates:({}-{}-{}). Writing them to the "
        + "database...",
        recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset()
    );

    try {
      writer.write(records);
    } catch (SQLException sqle) {
      log.warn(
          "Write of {} records failed, remainingRetries={}",
          records.size(),
          remainingRetries,
          sqle
      );
      String sqleAllMessages = "";
      for (Throwable e : sqle) {
        sqleAllMessages += e + System.lineSeparator();
      }
      if (remainingRetries == 0) {
        throw new ConnectException(new SQLException(sqleAllMessages));
      } else {
        writer.closeQuietly();
        writer = createWriter();
        remainingRetries--;
        context.timeout(config.retryBackoffMs);
        throw new RetriableException(new SQLException(sqleAllMessages));
      }
    }
    remainingRetries = config.maxRetries;
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
    // Not necessary
  }

  public void stop() {
    log.info("Stopping task");
    try {
      writer.closeQuietly();
    } finally {
      try {
        if (dialect != null) {
          dialect.close();
        }
      } catch (Throwable t) {
        log.warn("Error while closing the {} dialect: ", dialect.name(), t);
      } finally {
        dialect = null;
      }
    }
  }

  @Override
  public String version() {
    return getClass().getPackage().getImplementationVersion();
  }

}
