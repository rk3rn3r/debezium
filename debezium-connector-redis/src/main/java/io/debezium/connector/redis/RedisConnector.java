/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.redis;

import io.debezium.config.Configuration;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * The main connector class used to instantiate configuration and execution classes
 *
 * @author René Kerner (@rk3rn3r)
 *
 */
public class RedisConnector extends SourceConnector {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private Map<String, String> config;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public Config validate(Map<String, String> props) {
        Config config = super.validate(props);
        final Configuration debeziumConfig = Configuration.from(props);

        if (!debeziumConfig.validateAndRecord(RedisConnectorConfig.ALL_FIELDS, logger::error)) {
            throw new ConnectException("Error configuring an instance of " + getClass().getSimpleName() + "; check the logs for details");
        }

        return config;
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return RedisConnectorTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (maxTasks > 1) {
            throw new IllegalArgumentException("Only a single connector task may be started");
        }
        // @TODO
        //config.put("HALLO", "CONFIGURATION");
        return Collections.singletonList(config);
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return RedisConnectorConfig.configDef();
    }
}
