/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.redis;

import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;

/**
 * The topic naming strategy based on connector configuration and table name
 *
 * @author René Kerner (@rk3rn3r)
 *
 */
public class RedisTopicSelector {

    public static TopicSelector<TableId> defaultSelector(RedisConnectorConfig connectorConfig) {
        return TopicSelector.defaultSelector(
                connectorConfig,
                (tableId, prefix, delimiter) -> String.join(delimiter, prefix, tableId.schema())
        );
    }
}
