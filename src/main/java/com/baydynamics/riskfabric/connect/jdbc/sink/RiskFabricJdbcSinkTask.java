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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;

import com.baydynamics.riskfabric.connect.jdbc.dialect.RiskFabricDatabaseDialect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;

public class RiskFabricJdbcSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(RiskFabricJdbcSinkTask.class);

  DatabaseDialect dialect;
  RiskFabricJdbcSinkConfig config;
  String groupId;
  DbWriter writer;
  int remainingRetries;

  SinkTaskContext sinkTaskContext;
  Collection<TopicPartition> partitionAssignments;

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
    groupId = props.get("name");
    remainingRetries = config.maxRetries;
    dialect = DatabaseDialects.create("RiskFabricDatabaseDialect", config);
  }

  private DbWriter createWriter(String groupId, Collection<TopicPartition> partitionAssignements) {
    DbWriter newWriter;
    log.info("Creating writer for insert.mode {} using {} dialect", config.insertMode, dialect.getClass().getSimpleName());
    final DbStructure dbStructure = new DbStructure(dialect);
    if (config.insertMode.equals(JdbcSinkConfig.InsertMode.BULKCOPY)) {
      PgCopyWriter pgWriter = new PgCopyWriter(config, (RiskFabricDatabaseDialect) dialect, dbStructure);

      newWriter = pgWriter;

      if (config.bulkCopyDeliveryMode == JdbcSinkConfig.DeliveryMode.SYNCHRONIZED) {
        pgWriter.trackOffsets(groupId, partitionAssignements);

        HashMap<TopicPartition, Long> offsetMap = pgWriter.getOffsetMap();
        sinkTaskContext.offset(pgWriter.getOffsetMap());//synchronise offsets

        String syncMessage = "Synchronized partitions:" + System.lineSeparator();
        for (TopicPartition topicPartition : partitionAssignments) {
          syncMessage += String.format("* starting %s at offset [%d]" + System.lineSeparator(), topicPartition.toString(), offsetMap.get(topicPartition));
        }
        log.info(syncMessage);

      }
    }
    else {
      newWriter = new JdbcDbWriter(config, dialect, dbStructure);
    }
    return newWriter;
  }

  public void open(Collection<TopicPartition> partitions) {
    log.info("Open partitions");
    partitionAssignments = partitions; // save for later

    // open is called on each poll, so don't recreate the writer every time
    if (writer == null) {
      // open() is called after start() and open is where the partitions are known
      // with partitions known we can create the writer
      writer = createWriter(groupId, partitions);
    }

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
        writer = createWriter(groupId, partitionAssignments);
        log.info("New Writer of type {} created.", writer.getClass().getSimpleName());
        remainingRetries--;
        context.timeout(config.retryBackoffMs);

        // Connect will pause and attempt redelivery of the put batch
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