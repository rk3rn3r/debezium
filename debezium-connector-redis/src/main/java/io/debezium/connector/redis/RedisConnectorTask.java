/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.redis;

import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.cmd.impl.AppendCommand;
import com.moilioncircle.redis.replicator.cmd.impl.DelCommand;
import com.moilioncircle.redis.replicator.cmd.impl.GenericCommand;
import com.moilioncircle.redis.replicator.cmd.impl.GetSetCommand;
import com.moilioncircle.redis.replicator.cmd.impl.PSetExCommand;
import com.moilioncircle.redis.replicator.cmd.impl.SelectCommand;
import com.moilioncircle.redis.replicator.cmd.impl.SetCommand;
import com.moilioncircle.redis.replicator.cmd.impl.SetExCommand;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyStringValueString;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.data.Envelope;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.util.Collect;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * The main task executing streaming from SQL Server.
 * Responsible for lifecycle management the streaming code.
 *
 * @author René Kerner (@rk3rn3r)
 *
 */
public class RedisConnectorTask extends BaseSourceTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisConnectorTask.class);
    private static final String CONTEXT_NAME = "redis-connector-task";

    private Map<String, String> serverIdMap;
    private volatile RedisTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile ErrorHandler errorHandler;
    private final AtomicLong index = new AtomicLong(0);
    private final Schema keySchema = getKeySchema();
    private final Schema envelopeSchema = getEnvelope().schema();

    @Override
    public String version() {
        return Module.version();
    }

    private Map<Map<String, String>, Map<String, Object>> getLastOffsets() {
        Collection<Map<String, String>> partitions = new ArrayList<>();
        partitions.add(serverIdMap);
        return context.offsetStorageReader().offsets(partitions);
    }

    private void send(String replId, Long offset, Struct keyStruct, Struct valueStruct) {
        try {
            this.queue.enqueue(
                    new DataChangeEvent(
                        new SourceRecord(
                            serverIdMap,
                            Collect.hashMapOf("repl_id", replId, "redis_offset", offset),
                            "test-redis-01",
                            null,
                            keySchema,
                            keyStruct,
                            envelopeSchema,
                            valueStruct
                        )
                    )
            );
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void start(Configuration config) {
        final RedisConnectorConfig redisConfig = new RedisConnectorConfig(config);

        this.serverIdMap = Collections.singletonMap("server_id", redisConfig.getDsn());

        taskContext = new RedisTaskContext(
                redisConfig,
                RedisReplicationContext.getContext(serverIdMap, getLastOffsets())
        );

        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(redisConfig.getPollInterval())
                .maxBatchSize(redisConfig.getMaxBatchSize())
                .maxQueueSize(redisConfig.getMaxQueueSize())
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                .build();

        errorHandler = new ErrorHandler(RedisConnector.class, redisConfig.getLogicalName(), queue, this::cleanupResources);

        taskContext.getReplicator().addExceptionListener(
                (replicator, throwable, event) -> {
                    LOGGER.error("Error while processing event: {}, error: {}", event, throwable.getLocalizedMessage());
                });

        Envelope envelope = getEnvelope();

        taskContext.getReplicator().addEventListener(
                (replicator, event) -> {
                    LOGGER.debug("Redis event captured: {}", event);

                    if (event instanceof SelectCommand) {
                        this.index.set(((SelectCommand) event).getIndex());
                        return;
                    }
                    else if (event instanceof DelCommand) {
                        DelCommand delCmd = (DelCommand) event;
                        byte[][] keyArray = delCmd.getKeys();
                        for (int i = 0; i < keyArray.length; ++i) {
                            String delKey = null;
                            try {
                                delKey = new String(keyArray[i], "UTF-8");
                            } catch (UnsupportedEncodingException e) {
                                e.printStackTrace();
                            }

                            LOGGER.debug("Captured delete for key: {}", delKey);

                            Struct keyStruct = getKeyStruct(delKey);
                            long offset;
                            if (i == keyArray.length - 1) { // when we send the last delete
                                offset = delCmd.getOffsetEnd(); // commit the end offset
                            }
                            else { // only send the start offset, so we can re-send on error
                                offset = delCmd.getOffsetStart();
                            }

                            send(
                                replicator.getConfiguration().getReplId(), offset,
                                keyStruct,
                                getEnvelope().delete(null, null, null)
                            );
                        }
                        return;
                    }

                    Struct keyStruct = null;
                    Struct valueStruct = null;
                    try {
                        if (event instanceof KeyStringValueString) { // FULLSYNC
                            KeyStringValueString data = (KeyStringValueString) event;
                            keyStruct = getKeyStruct(data.getDb().getDbNumber(), new String(data.getKey(), "UTF-8"));
                            valueStruct = envelope.create(new String(data.getValue(), "UTF-8"), null, null);
                        }
                        else if (event instanceof SetCommand) {
                            SetCommand data = (SetCommand) event;
                            keyStruct = getKeyStruct(new String(data.getKey(), "UTF-8"));
                            valueStruct = envelope.create(new String(data.getValue(), "UTF-8"), null, null);
                        }
                        else if (event instanceof SetExCommand) {
                            SetExCommand data = (SetExCommand) event;
                            keyStruct = getKeyStruct(new String(data.getKey(), "UTF-8"));
                            valueStruct = envelope.create(new String(data.getValue(), "UTF-8"), null, null);
                        }
                        else if (event instanceof PSetExCommand) {
                            PSetExCommand data = (PSetExCommand) event;
                            keyStruct = getKeyStruct(new String(data.getKey(), "UTF-8"));
                            valueStruct = envelope.create(new String(data.getValue(), "UTF-8"), null, null);
                        }
                        else if (event instanceof GetSetCommand) {
                            GetSetCommand data = (GetSetCommand) event;
                            keyStruct = getKeyStruct(new String(data.getKey(), "UTF-8"));
                            valueStruct = envelope.create(new String(data.getValue(), "UTF-8"), null, null);
                        }
//                        else if (event instanceof AppendCommand) { // @FIXME needs a new APPEND (a) OPERATION for envelope.Operation
//                            AppendCommand data = (AppendCommand) event; // @TODO won't support APPEND right now
//                            keyStruct = getKeyStruct(new String(data.getKey(), "UTF-8"));
//                            valueStruct = envelope.create(new String(data.getValue(), "UTF-8"), null, null);
//                        }
                        else {
                            LOGGER.debug("Unhandled Redis event captured: {}", event);
                            return;
                        }
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }

                    if (null == valueStruct || null == keyStruct) { // this should never happen!
                        return;
                    }

                    long newOffset;
                    if (event instanceof GenericCommand) {
                        newOffset = ((GenericCommand) event).getOffsetEnd();
                    } else {
                        LOGGER.warn("Couldn't get endOffset for command {}! Falling back to global offset.", event);
                        newOffset = replicator.getConfiguration().getReplOffset();
                    }

                    send(
                        replicator.getConfiguration().getReplId(), newOffset,
                        keyStruct,
                        valueStruct
                    );
                });
            Executors.newSingleThreadExecutor().execute(() ->
            {
                try {
                    taskContext.getReplicator().open();
                } catch (IOException e) {
                    throw new ConnectException("Error opening connection to Redis!", e);
                }
                    LOGGER.info("RedisReplicator started!");
            });
    }

    private Struct getKeyStruct(String key) {
        return getKeyStruct(null, key);
    }

    private Struct getKeyStruct(Long index, String key) {
        Struct keyStruct = new Struct(keySchema);
        keyStruct.put("index", index == null ? this.index.get() : index).put("id", key);
        return keyStruct;
    }

    private Schema getKeySchema() {
        return SchemaBuilder.struct()
                    .name("Redis.Schema.Key")
                    .field("index", Schema.INT64_SCHEMA)
                    .field("id", Schema.STRING_SCHEMA)
                    .build();
    }

    private Envelope getEnvelope() {
        return Envelope.defineSchema()
            .withName("Redis.Schema.Envelope")
            .withRecord(Schema.OPTIONAL_STRING_SCHEMA)
            .withSource(Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    }

    /**
     * Loads the connector's persistent offset (if present) via the given loader.
     */
//    @Override
//    protected OffsetContext getPreviousOffset(OffsetContext.Loader loader) { // @TODO use that one?!
//        Map<String, ?> partition = loader.getPartition();
//
//        Map<String, Object> previousOffset = context.offsetStorageReader()
//                .offsets(Collections.singleton(partition))
//                .get(partition);
//
//        if (previousOffset != null) {
//            OffsetContext offsetContext = loader.load(previousOffset);
//            LOGGER.info("Found previous offset {}", offsetContext);
//            return offsetContext;
//        }
//        else {
//            return null;
//        }
//    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        return queue.poll().stream()
            .map(DataChangeEvent::getRecord)
            .collect(Collectors.toList());
    }

    @Override
    public void stop() {
        cleanupResources();
    }

    private void cleanupResources() {
        try {
            if (errorHandler != null) {
                errorHandler.stop();
            }
        }
        catch (InterruptedException e) {
            Thread.interrupted();
            LOGGER.error("Interrupted while stopping", e);
        }
        if (taskContext != null && taskContext.getReplicator() != null) {
            try {
                taskContext.getReplicator().close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return RedisConnectorConfig.ALL_FIELDS;
    }
}
