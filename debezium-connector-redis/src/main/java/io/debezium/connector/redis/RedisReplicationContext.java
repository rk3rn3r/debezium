/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.redis;

import java.util.Map;

/**
 *
 * @author René Kerner (@rk3rn3r)
 *
 */
class RedisReplicationContext {

    private String lastReplId = null;
    private Long lastOffset = null;

    private RedisReplicationContext() {
    }

    static RedisReplicationContext getContext(Map<String, String> serverIdMap, Map<Map<String, String>, Map<String, Object>> offsets) {
        RedisReplicationContext context = new RedisReplicationContext();
        Map<String, Object> offsetsMap = offsets.get(serverIdMap);
        if (offsetsMap != null) {
            context.lastReplId = (String) offsetsMap.get("repl_id");
            context.lastOffset = (Long) offsetsMap.get("redis_offset");
        }
        return context;
    }

    String getLastReplId() {
        return lastReplId;
    }

    Long getLastOffset() {
        return lastOffset;
    }
}
