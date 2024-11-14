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

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.annotation.VisibleForTesting;
import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.data.StringData;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.FlushEvent;
import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.common.event.SchemaChangeEventType;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.pipeline.SchemaChangeBehavior;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.schema.Selectors;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DataTypeFamily;
import com.ververica.cdc.common.types.DataTypeRoot;
import com.ververica.cdc.common.utils.ChangeEventUtils;
import com.ververica.cdc.runtime.operators.schema.coordinator.SchemaRegistry;
import com.ververica.cdc.runtime.operators.schema.event.CoordinationResponseUtils;
import com.ververica.cdc.runtime.operators.schema.event.SchemaChangeProcessingResponse;
import com.ververica.cdc.runtime.operators.schema.event.SchemaChangeRequest;
import com.ververica.cdc.runtime.operators.schema.event.SchemaChangeResponse;
import com.ververica.cdc.runtime.operators.schema.event.SchemaChangeResultRequest;
import com.ververica.cdc.runtime.operators.schema.event.SchemaChangeResultResponse;
import com.ververica.cdc.runtime.operators.sink.SchemaEvolutionClient;
import com.ververica.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheLoader;
import org.apache.flink.shaded.guava30.com.google.common.cache.LoadingCache;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.SerializedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * The operator will evolve schemas in {@link SchemaRegistry} for incoming {@link
 * SchemaChangeEvent}s and block the stream for tables before their schema changes finish.
 */
@Internal
public class SchemaOperator extends AbstractStreamOperator<Event>
        implements OneInputStreamOperator<Event, Event> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SchemaOperator.class);
    private static final Duration CACHE_EXPIRE_DURATION = Duration.ofDays(1);
    private final List<Tuple2<String, TableId>> routingRules;
    private transient List<Tuple2<Selectors, TableId>> routes;
    private transient TaskOperatorEventGateway toCoordinator;
    private transient SchemaEvolutionClient schemaEvolutionClient;
    private transient LoadingCache<TableId, Schema> cachedSchemas;
    private final long rpcTimeOutInMillis;
    private final SchemaChangeBehavior schemaChangeBehavior;
    private transient int subTaskId;

    @VisibleForTesting
    public SchemaOperator(Duration rpcTimeOut, List<Tuple2<String, TableId>> routingRules) {
        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.rpcTimeOutInMillis = rpcTimeOut.toMillis();
        this.schemaChangeBehavior = SchemaChangeBehavior.EVOLVE;
        this.routingRules = routingRules;
    }

    public SchemaOperator(Duration rpcTimeOut, SchemaChangeBehavior schemaChangeBehavior, List<Tuple2<String, TableId>> routingRules) {
        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.rpcTimeOutInMillis = rpcTimeOut.toMillis();
        this.schemaChangeBehavior = schemaChangeBehavior;
        this.routingRules = routingRules;
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
        routes =
                routingRules.stream()
                        .map(
                                tuple2 -> {
                                    String tableInclusions = tuple2.f0;
                                    TableId replaceBy = tuple2.f1;
                                    Selectors selectors =
                                            new Selectors.SelectorsBuilder()
                                                    .includeTables(tableInclusions)
                                                    .build();
                                    return new Tuple2<>(selectors, replaceBy);
                                })
                        .collect(Collectors.toList());
        schemaEvolutionClient = new SchemaEvolutionClient(toCoordinator, getOperatorID());
        cachedSchemas =
                CacheBuilder.newBuilder()
                        .expireAfterAccess(CACHE_EXPIRE_DURATION)
                        .build(
                                new CacheLoader<TableId, Schema>() {
                                    @Override
                                    public Schema load(TableId tableId) {
                                        return getLatestSchema(tableId);
                                    }
                                });
    }

    /**
     * This method is guaranteed to not be called concurrently with other methods of the operator.
     */
    @Override
    public void processElement(StreamRecord<Event> streamRecord)
            throws InterruptedException, TimeoutException, ExecutionException {
        Event event = streamRecord.getValue();
        // Schema changes
        if (event instanceof SchemaChangeEvent) {
            TableId tableId = ((SchemaChangeEvent) event).tableId();
            processSchemaChangeEvents((SchemaChangeEvent) event);
            // Update caches
            cachedSchemas.put(tableId, getLatestSchema(tableId));
            getRoutedTable(tableId)
                    .ifPresent(routed -> cachedSchemas.put(routed, getLatestSchema(routed)));
        } else if (event instanceof DataChangeEvent) {
            // Data changes
            DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
            TableId tableId = dataChangeEvent.tableId();
            Optional<TableId> optionalRoutedTable = getRoutedTable(tableId);
            if (optionalRoutedTable.isPresent()) {
                output.collect(
                        new StreamRecord<>(
                                maybeFillInNullForEmptyColumns(
                                        dataChangeEvent, optionalRoutedTable.get())));
            } else {
                output.collect(streamRecord);
            }
        } else {
            throw new RuntimeException("Unknown event type in Stream record: " + event);
        }
    }

    private DataChangeEvent maybeFillInNullForEmptyColumns(
            DataChangeEvent originalEvent, TableId routedTableId) {
        try {
            Schema originalSchema = cachedSchemas.get(originalEvent.tableId());
            Schema routedTableSchema = cachedSchemas.get(routedTableId);
            if (originalSchema.equals(routedTableSchema)) {
                return ChangeEventUtils.recreateDataChangeEvent(originalEvent, routedTableId);
            }
            switch (originalEvent.op()) {
                case INSERT:
                    return DataChangeEvent.insertEvent(
                            routedTableId,
                            regenerateRecordData(
                                    originalEvent.after(), originalSchema, routedTableSchema),
                            originalEvent.meta());
                case UPDATE:
                    return DataChangeEvent.updateEvent(
                            routedTableId,
                            regenerateRecordData(
                                    originalEvent.before(), originalSchema, routedTableSchema),
                            regenerateRecordData(
                                    originalEvent.after(), originalSchema, routedTableSchema),
                            originalEvent.meta());
                case DELETE:
                    return DataChangeEvent.deleteEvent(
                            routedTableId,
                            regenerateRecordData(
                                    originalEvent.before(), originalSchema, routedTableSchema),
                            originalEvent.meta());
                case REPLACE:
                    return DataChangeEvent.replaceEvent(
                            routedTableId,
                            regenerateRecordData(
                                    originalEvent.after(), originalSchema, routedTableSchema),
                            originalEvent.meta());
                default:
                    throw new IllegalArgumentException(
                            String.format(
                                    "Unrecognized operation type \"%s\"", originalEvent.op()));
            }
        } catch (Exception e) {
            throw new IllegalStateException("Unable to fill null for empty columns", e);
        }
    }

    private RecordData regenerateRecordData(
            RecordData recordData, Schema originalSchema, Schema routedTableSchema) {
        // Regenerate record data
        List<RecordData.FieldGetter> fieldGetters = new ArrayList<>();
        for (Column column : routedTableSchema.getColumns()) {
            String columnName = column.getName();
            int columnIndex = originalSchema.getColumnNames().indexOf(columnName);
            if (columnIndex == -1) {
                fieldGetters.add(new NullFieldGetter());
            } else {
                RecordData.FieldGetter fieldGetter =
                        RecordData.createFieldGetter(
                                originalSchema.getColumn(columnName).get().getType(), columnIndex);
                // Check type compatibility
                if (originalSchema.getColumn(columnName).get().getType().equals(column.getType())) {
                    fieldGetters.add(fieldGetter);
                } else {
                    fieldGetters.add(new TypeCoercionFieldGetter(column.getType(), fieldGetter));
                }
            }
        }
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(
                        routedTableSchema.getColumnDataTypes().toArray(new DataType[0]));
        return recordDataGenerator.generate(
                fieldGetters.stream()
                        .map(fieldGetter -> fieldGetter.getFieldOrNull(recordData))
                        .toArray());
    }

    private Optional<TableId> getRoutedTable(TableId originalTableId) {
        for (Tuple2<Selectors, TableId> route : routes) {
            if (route.f0.isMatch(originalTableId)) {
                return Optional.of(route.f1);
            }
        }
        return Optional.empty();
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

    private Schema getLatestSchema(TableId tableId) {
        try {
            Optional<Schema> optionalSchema = schemaEvolutionClient.getLatestSchema(tableId);
            if (!optionalSchema.isPresent()) {
                throw new IllegalStateException(
                        String.format("Schema doesn't exist for table \"%s\"", tableId));
            }
            return optionalSchema.get();
        } catch (Exception e) {
            throw new IllegalStateException(
                    String.format("Unable to get latest schema for table \"%s\"", tableId));
        }
    }

    private static class NullFieldGetter implements RecordData.FieldGetter {
        @Nullable
        @Override
        public Object getFieldOrNull(RecordData recordData) {
            return null;
        }
    }

    private static class TypeCoercionFieldGetter implements RecordData.FieldGetter {
        private final DataType destinationType;
        private final RecordData.FieldGetter originalFieldGetter;

        public TypeCoercionFieldGetter(
                DataType destinationType, RecordData.FieldGetter originalFieldGetter) {
            this.destinationType = destinationType;
            this.originalFieldGetter = originalFieldGetter;
        }

        @Nullable
        @Override
        public Object getFieldOrNull(RecordData recordData) {
            Object originalField = originalFieldGetter.getFieldOrNull(recordData);
            if (originalField == null) {
                return null;
            }
            if (destinationType.is(DataTypeRoot.BIGINT)) {
                if (originalField instanceof Byte) {
                    // TINYINT
                    return ((Byte) originalField).longValue();
                } else if (originalField instanceof Short) {
                    // SMALLINT
                    return ((Short) originalField).longValue();
                } else if (originalField instanceof Integer) {
                    // INT
                    return ((Integer) originalField).longValue();
                } else {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Cannot fit type \"%s\" into a BIGINT column. "
                                            + "Currently only TINYINT / SMALLINT / INT can be accepted by a BIGINT column",
                                    originalField.getClass()));
                }
            } else if (destinationType.is(DataTypeFamily.APPROXIMATE_NUMERIC)) {
                if (originalField instanceof Float) {
                    // FLOAT
                    return ((Float) originalField).doubleValue();
                } else {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Cannot fit type \"%s\" into a DOUBLE column. "
                                            + "Currently only FLOAT can be accepted by a DOUBLE column",
                                    originalField.getClass()));
                }
            } else if (destinationType.is(DataTypeRoot.VARCHAR)) {
                if (originalField instanceof StringData) {
                    return originalField;
                } else {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Cannot fit type \"%s\" into a STRING column. "
                                            + "Currently only CHAR / VARCHAR can be accepted by a STRING column",
                                    originalField.getClass()));
                }
            } else {
                throw new IllegalArgumentException(
                        String.format(
                                "Column type \"%s\" doesn't support type coercion",
                                destinationType));
            }
        }
    }
}
