/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.redis;

import com.moilioncircle.redis.replicator.RedisURI;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.history.KafkaDatabaseHistory;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Width;

import java.net.URISyntaxException;

/**
 * The list of configuration options for SQL Server connector
 *
 * @author René Kerner (@rk3rn3r)
 */
public class RedisConnectorConfig extends CommonConnectorConfig {

    /**
     * The set of predefined SnapshotMode options or aliases.
     */
    public static enum SnapshotMode implements EnumeratedValue {

        /**
         * Perform a snapshot of data when there is no valid offset for the connector.
         */
        AUTO("auto"),

        /**
         * Force a snapshot of data and schema upon initial startup of a connector.
         */
        FORCE("force"),

        /**
         * Start capturing only new data.
         */
        NONE("none");

        private final String value;

        private SnapshotMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static SnapshotMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();

            for (SnapshotMode option : SnapshotMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }

            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static SnapshotMode parse(String value, String defaultValue) {
            SnapshotMode mode = parse(value);

            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }

            return mode;
        }
    }

    public static final Field DSN = Field.create("redis.dsn")
            .withDisplayName("RedisReplicator DSN")
            .withType(ConfigDef.Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withValidation(
                    Field::isRequired,
                    (config, field, problems) -> {
                        int errors = 0;
                        try {
                            new RedisURI(config.getString(field));
                        } catch (URISyntaxException e) {
                            errors += 1;
                            problems.accept(field, config.getString(field), e.getMessage());
                        }
                        return errors;
                    })
            .withDescription("The RedisReplicator DSN.");

    public static final Field SNAPSHOT_MODE = Field.create("snapshot.mode")
            .withDisplayName("Snapshot mode")
            .withEnum(SnapshotMode.class, SnapshotMode.AUTO)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("The criteria for running a snapshot upon startup of the connector. "
                    + "Options include: "
                    + "'auto' (the default) to specify the connector should run a snapshot only when no offsets are available for the master replication id; "
                    + "'none' don't snapshot and just start reading new data. ");

    /**
     * The set of {@link Field}s defined as part of this configuration.
     */
    public static Field.Set ALL_FIELDS = Field.setOf(
            DSN,
            SNAPSHOT_MODE,
            RelationalDatabaseConnectorConfig.TABLE_WHITELIST,
            RelationalDatabaseConnectorConfig.TABLE_BLACKLIST,
            CommonConnectorConfig.POLL_INTERVAL_MS,
            CommonConnectorConfig.MAX_BATCH_SIZE,
            CommonConnectorConfig.MAX_QUEUE_SIZE,
            CommonConnectorConfig.SNAPSHOT_DELAY_MS,
            CommonConnectorConfig.SNAPSHOT_FETCH_SIZE,
            Heartbeat.HEARTBEAT_INTERVAL, Heartbeat.HEARTBEAT_TOPICS_PREFIX,
            CommonConnectorConfig.SOURCE_STRUCT_MAKER_VERSION
    );

    public static ConfigDef configDef() {
        ConfigDef config = new ConfigDef();

        Field.group(config, "Redis Server", DSN, SNAPSHOT_MODE);
        Field.group(config, "History Storage", KafkaDatabaseHistory.BOOTSTRAP_SERVERS,
                KafkaDatabaseHistory.TOPIC, KafkaDatabaseHistory.RECOVERY_POLL_ATTEMPTS,
                KafkaDatabaseHistory.RECOVERY_POLL_INTERVAL_MS);
        Field.group(config, "Events", RelationalDatabaseConnectorConfig.TABLE_WHITELIST,
                RelationalDatabaseConnectorConfig.TABLE_BLACKLIST,
                Heartbeat.HEARTBEAT_INTERVAL, Heartbeat.HEARTBEAT_TOPICS_PREFIX,
                CommonConnectorConfig.SOURCE_STRUCT_MAKER_VERSION
        );
        Field.group(config, "Connector", CommonConnectorConfig.POLL_INTERVAL_MS, CommonConnectorConfig.MAX_BATCH_SIZE,
                CommonConnectorConfig.MAX_QUEUE_SIZE, CommonConnectorConfig.SNAPSHOT_DELAY_MS, CommonConnectorConfig.SNAPSHOT_FETCH_SIZE);

        return config;
    }

    private final SnapshotMode snapshotMode;
    private final String dsn;

    public RedisConnectorConfig(Configuration config) {
        super(config, config.getString(DSN), 1_000);
        this.dsn = config.getString(DSN);
        this.snapshotMode = SnapshotMode.parse(config.getString(SNAPSHOT_MODE), SNAPSHOT_MODE.defaultValueAsString());

    }

    public SnapshotMode getSnapshotMode() {
        return snapshotMode;
    }

    public String getDsn() {
        return dsn;
    }

    @Override
    protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
        return null;
    }

}
