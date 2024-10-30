/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.runtime.operators.schema;

import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.SerializedValue;

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.annotation.VisibleForTesting;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.FlushEvent;
import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.common.event.SchemaChangeEventType;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.pipeline.SchemaChangeBehavior;
import com.ververica.cdc.runtime.operators.schema.coordinator.SchemaRegistry;
import com.ververica.cdc.runtime.operators.schema.event.CoordinationResponseUtils;
import com.ververica.cdc.runtime.operators.schema.event.SchemaChangeProcessingResponse;
import com.ververica.cdc.runtime.operators.schema.event.SchemaChangeRequest;
import com.ververica.cdc.runtime.operators.schema.event.SchemaChangeResponse;
import com.ververica.cdc.runtime.operators.schema.event.SchemaChangeResultRequest;
import com.ververica.cdc.runtime.operators.schema.event.SchemaChangeResultResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static com.ververica.cdc.common.pipeline.PipelineOptions.DEFAULT_SCHEMA_OPERATOR_RPC_TIMEOUT;

/**
 * The operator will evolve schemas in {@link SchemaRegistry} for incoming {@link
 * SchemaChangeEvent}s and block the stream for tables before their schema changes finish.
 */
@Internal
public class SchemaOperator extends AbstractStreamOperator<Event>
        implements OneInputStreamOperator<Event, Event> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SchemaOperator.class);
    private transient TaskOperatorEventGateway toCoordinator;
    private final long rpcTimeOutInMillis;
    private final SchemaChangeBehavior schemaChangeBehavior;
    private transient int subTaskId;

    @VisibleForTesting
    public SchemaOperator() {
        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.rpcTimeOutInMillis = DEFAULT_SCHEMA_OPERATOR_RPC_TIMEOUT.toMillis();
        this.schemaChangeBehavior = SchemaChangeBehavior.EVOLVE;
    }

    @VisibleForTesting
    public SchemaOperator(Duration rpcTimeOut) {
        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.rpcTimeOutInMillis = rpcTimeOut.toMillis();
        this.schemaChangeBehavior = SchemaChangeBehavior.EVOLVE;
    }

    public SchemaOperator(Duration rpcTimeOut, SchemaChangeBehavior schemaChangeBehavior) {
        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.rpcTimeOutInMillis = rpcTimeOut.toMillis();
        this.schemaChangeBehavior = schemaChangeBehavior;
    }

    @Override
    public void open() throws Exception {
        super.open();
        subTaskId = getRuntimeContext().getIndexOfThisSubtask();
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<Event>> output) {
        super.setup(containingTask, config, output);
        this.toCoordinator = containingTask.getEnvironment().getOperatorCoordinatorEventGateway();
    }

    /**
     * This method is guaranteed to not be called concurrently with other methods of the operator.
     */
    @Override
    public void processElement(StreamRecord<Event> streamRecord)
            throws InterruptedException, TimeoutException, ExecutionException {
        Event event = streamRecord.getValue();
        if (event instanceof SchemaChangeEvent) {
            processSchemaChangeEvents((SchemaChangeEvent) event);
        } else if (event instanceof DataChangeEvent) {
            output.collect(streamRecord);
        } else {
            throw new RuntimeException("Unknown event type in Stream record: " + event);
        }
    }

    private void processSchemaChangeEvents(SchemaChangeEvent event)
            throws InterruptedException, TimeoutException {
        TableId tableId = event.tableId();
        LOG.info(
                "{}> Table {} received SchemaChangeEvent {} and start to be blocked.",
                subTaskId,
                tableId,
                event);
        handleSchemaChangeEvent(tableId, event);
    }

    private void handleSchemaChangeEvent(TableId tableId, SchemaChangeEvent schemaChangeEvent)
            throws InterruptedException, TimeoutException {
        if (schemaChangeBehavior == SchemaChangeBehavior.EXCEPTION
                && schemaChangeEvent.getType() != SchemaChangeEventType.CREATE_TABLE) {
            // CreateTableEvent should be applied even in EXCEPTION mode
            throw new RuntimeException(
                    String.format(
                            "Refused to apply schema change event %s in EXCEPTION mode.",
                            schemaChangeEvent));
        }

        // The request will block if another schema change event is being handled
        SchemaChangeResponse response = requestSchemaChange(tableId, schemaChangeEvent);
        if (response.isAccepted()) {
            LOG.info("{}> Sending the FlushEvent for table {}.", subTaskId, tableId);
            output.collect(new StreamRecord<>(new FlushEvent(tableId)));

            // The request will block until flushing finished in each sink writer
            SchemaChangeResultResponse schemaEvolveResponse = requestSchemaChangeResult();
            List<SchemaChangeEvent> finishedSchemaChangeEvents =
                    schemaEvolveResponse.getFinishedSchemaChangeEvents();

            // Update evolved schema changes based on apply results
            finishedSchemaChangeEvents.forEach(e -> output.collect(new StreamRecord<>(e)));
        } else if (response.isDuplicate()) {
            LOG.info(
                    "{}> Schema change event {} has been handled in another subTask already.",
                    subTaskId,
                    schemaChangeEvent);
        } else if (response.isIgnored()) {
            LOG.info(
                    "{}> Schema change event {} has been ignored. No schema evolution needed.",
                    subTaskId,
                    schemaChangeEvent);
        } else {
            throw new IllegalStateException("Unexpected response status " + response);
        }
    }

    private SchemaChangeResponse requestSchemaChange(
            TableId tableId, SchemaChangeEvent schemaChangeEvent)
            throws InterruptedException, TimeoutException {
        long schemaEvolveTimeOutMillis = System.currentTimeMillis() + rpcTimeOutInMillis;
        while (true) {
            SchemaChangeResponse response =
                    sendRequestToCoordinator(new SchemaChangeRequest(tableId, schemaChangeEvent));
            if (response.isRegistryBusy()) {
                if (System.currentTimeMillis() < schemaEvolveTimeOutMillis) {
                    LOG.info(
                            "{}> Schema Registry is busy now, waiting for next request...",
                            subTaskId);
                    Thread.sleep(1000);
                } else {
                    throw new TimeoutException("TimeOut when requesting schema change");
                }
            } else {
                return response;
            }
        }
    }

    private SchemaChangeResultResponse requestSchemaChangeResult()
            throws InterruptedException, TimeoutException {
        CoordinationResponse coordinationResponse =
                sendRequestToCoordinator(new SchemaChangeResultRequest());
        long nextRpcTimeOutMillis = System.currentTimeMillis() + rpcTimeOutInMillis;
        while (coordinationResponse instanceof SchemaChangeProcessingResponse) {
            if (System.currentTimeMillis() < nextRpcTimeOutMillis) {
                Thread.sleep(1000);
                coordinationResponse = sendRequestToCoordinator(new SchemaChangeResultRequest());
            } else {
                throw new TimeoutException("TimeOut when requesting release upstream");
            }
        }
        return ((SchemaChangeResultResponse) coordinationResponse);
    }

    private <REQUEST extends CoordinationRequest, RESPONSE extends CoordinationResponse>
            RESPONSE sendRequestToCoordinator(REQUEST request) {
        try {
            CompletableFuture<CoordinationResponse> responseFuture =
                    toCoordinator.sendRequestToCoordinator(
                            getOperatorID(), new SerializedValue<>(request));
            return CoordinationResponseUtils.unwrap(responseFuture.get());
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to send request to coordinator: " + request.toString(), e);
        }
    }
}
