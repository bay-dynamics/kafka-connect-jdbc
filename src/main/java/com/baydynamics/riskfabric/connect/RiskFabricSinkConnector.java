package com.baydynamics.riskfabric.connect;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.confluent.connect.jdbc.sink.JdbcSinkTask;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.util.Version;

public class RiskFabricSinkConnector extends SinkConnector {
    private static final Logger log = LoggerFactory.getLogger(RiskFabricSinkConnector.class);

    private Map<String, String> configProps;

    public Class<? extends Task> taskClass() {
        return JdbcSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("Setting task configurations for {} workers.", maxTasks);
        final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; ++i) {
            configs.add(configProps);
        }
        return configs;
    }

    @Override
    public void start(Map<String, String> props) {

        //@TODO force properties to ensure RiskFabricDialect

        configProps = props;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return JdbcSinkConfig.CONFIG_DEF;
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        return super.validate(connectorConfigs);
    }

    @Override
    public String version() {
        return Version.getVersion();
    }
}
