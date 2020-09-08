/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.endpoint;

import java.util.List;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;

@Path("/debezium")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class DebeziumResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumResource.class);

    private static final TypeReference<List<Map<String, String>>> TASK_CONFIGS_TYPE = new TypeReference<List<Map<String, String>>>() {
    };

    // TODO: This should not be so long. However, due to potentially long rebalances that may have to wait a full
    // session timeout to complete, during which we cannot serve some requests. Ideally we could reduce this, but
    // we need to consider all possible scenarios this could fail. It might be ok to fail with a timeout in rare cases,
    // but currently a worker simply leaving the group can take this long as well.
    public static final long REQUEST_TIMEOUT_MS = 90 * 1000;
    // Mutable for integration testing; otherwise, some tests would take at least REQUEST_TIMEOUT_MS
    // to run
    private static long requestTimeoutMs = REQUEST_TIMEOUT_MS;

    @javax.ws.rs.core.Context
    private ServletContext context;

    public DebeziumResource() {
    }

    // For testing purposes only
    public static void setRequestTimeout(long requestTimeoutMs) {
        DebeziumResource.requestTimeoutMs = requestTimeoutMs;
    }

    public static void resetRequestTimeout() {
        DebeziumResource.requestTimeoutMs = REQUEST_TIMEOUT_MS;
    }

    @GET
    @Path("/hello")
    public Response helloDebezium(
                                  final @Context UriInfo uriInfo,
                                  final @Context HttpHeaders headers) {
        return Response.ok("Hello Debezium").build();
    }
}
