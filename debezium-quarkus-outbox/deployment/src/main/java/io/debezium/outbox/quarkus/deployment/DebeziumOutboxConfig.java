/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.deployment;

import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;

/**
 * Configuration root class for Debezium Outbox pattern that defines the available user
 * configuration options to customize this extension's behavior.
 *
 * @author Chris Cranford
 */
@ConfigMapping(prefix = "quarkus.debezium-outbox")
@ConfigRoot(phase = ConfigPhase.BUILD_TIME)
public interface DebeziumOutboxConfig extends DebeziumOutboxCommonConfig {

}
