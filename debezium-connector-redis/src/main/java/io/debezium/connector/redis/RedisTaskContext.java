/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.redis;

import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.RedisSocketReplicator;
import com.moilioncircle.redis.replicator.RedisURI;
import io.debezium.connector.common.CdcSourceTaskContext;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * A state (context) associated with a Redis task
 *
 * @author René Kerner (@rk3rn3r)
 *
 */
class RedisTaskContext extends CdcSourceTaskContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisTaskContext.class);

    private static final String REDIS_INFO_SECTION_REPLICATION = "replication";
    private static final String REDIS_MASTER_REPL_ID = "master_replid";
    private static final String REDIS_MASTER_REPL_OFFSET = "master_repl_offset";

    private RedisSocketReplicator replicator;

    RedisTaskContext(RedisConnectorConfig config, RedisReplicationContext redisReplicationContext) {
        super("Redis", config.getLogicalName(), null);
        RedisURI uri = getRedisURI(config);
        Configuration replicatorConfig = Configuration.valueOf(uri);

        Map<String, String> redisServerReplicationConfig = getRedisReplicationInfo(uri, replicatorConfig);

        LOGGER.debug("Redis server repl_id: {}", redisServerReplicationConfig.get(REDIS_MASTER_REPL_ID));
        replicatorConfig.setReplId(redisServerReplicationConfig.get(REDIS_MASTER_REPL_ID));

        String lastReplId = redisReplicationContext.getLastReplId();
        if (RedisConnectorConfig.SnapshotMode.AUTO.equals(config.getSnapshotMode())) {
            Long lastOffset = redisReplicationContext.getLastOffset();
            if (lastOffset == null) {
                lastOffset = -1L;
            }
            if (null == lastReplId) {
                lastOffset = -1L;
            } else {
                replicatorConfig.setReplId(lastReplId);
            }
            replicatorConfig.setReplOffset(lastOffset);
        }
        else if (RedisConnectorConfig.SnapshotMode.FORCE.equals(config.getSnapshotMode())) {
            LOGGER.info("SnapshotMode=FORCE Forcing snapshot!");
            replicatorConfig.setReplOffset(-1L);
        }
        else if (RedisConnectorConfig.SnapshotMode.NONE.equals(config.getSnapshotMode())) { // @FIXME need to be validated if that actually works
            LOGGER.info("SnapshotMode=NONE Skipping snapshot and just start consuming new events...");
            LOGGER.debug("Skipping snapshotting and start at offset: {}", redisServerReplicationConfig.get(REDIS_MASTER_REPL_OFFSET));
            replicatorConfig.setReplOffset(Long.valueOf(redisServerReplicationConfig.get(REDIS_MASTER_REPL_OFFSET)));
        }

        this.replicator = new RedisSocketReplicator(
                uri.getHost(),
                uri.getPort(),
                replicatorConfig
        );

        this.configureLoggingContext(config.getDsn());
    }

    private RedisURI getRedisURI(RedisConnectorConfig config) {
        RedisURI uri;
        try {
            uri = new RedisURI(config.getDsn());
        } catch (URISyntaxException e) {
            throw new ConnectException("Incorrect redis DSN: " + config.getDsn(), e);
        }
        return uri;
    }

    private Map<String, String> getRedisReplicationInfo(RedisURI uri, Configuration replicatorConfig) {
        Jedis jedis = new Jedis(uri.getHost(), uri.getPort());
        if (
             replicatorConfig.getAuthPassword() != null
             && !replicatorConfig.getAuthPassword().isEmpty()
        ) {
            jedis.auth(replicatorConfig.getAuthPassword());
        }
        String replicationInfo = jedis.info(REDIS_INFO_SECTION_REPLICATION);
        jedis.close();

        Map<String, String> redisServerReplicationConfig = new HashMap<>();
        for (String line : replicationInfo.split("\n") ) {
            String[] keyValuePair = line.split(":");
            if (keyValuePair.length == 2) {
                redisServerReplicationConfig.put(keyValuePair[0].trim(), keyValuePair[1].trim());
            }
        }
        return redisServerReplicationConfig;
    }

    RedisSocketReplicator getReplicator() {
        return replicator;
    }
}
