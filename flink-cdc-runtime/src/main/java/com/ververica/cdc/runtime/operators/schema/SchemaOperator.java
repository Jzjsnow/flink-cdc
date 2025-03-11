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

import org.apache.flink.api.java.tuple.Tuple2;
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

import org.apache.flink.shaded.guava30.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheLoader;
import org.apache.flink.shaded.guava30.com.google.common.cache.LoadingCache;

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.annotation.VisibleForTesting;
import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.data.StringData;
import com.ververica.cdc.common.event.AddColumnEvent;
import com.ververica.cdc.common.event.CreateTableEvent;
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
import com.ververica.cdc.common.source.SupportedMetadataColumn;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DataTypeFamily;
import com.ververica.cdc.common.types.DataTypeRoot;
import com.ververica.cdc.common.utils.ChangeEventUtils;
import com.ververica.cdc.common.utils.SchemaUtils;
import com.ververica.cdc.runtime.operators.schema.coordinator.SchemaRegistry;
import com.ververica.cdc.runtime.operators.schema.event.CoordinationResponseUtils;
import com.ververica.cdc.runtime.operators.schema.event.SchemaChangeProcessingResponse;
import com.ververica.cdc.runtime.operators.schema.event.SchemaChangeRequest;
import com.ververica.cdc.runtime.operators.schema.event.SchemaChangeResponse;
import com.ververica.cdc.runtime.operators.schema.event.SchemaChangeResultRequest;
import com.ververica.cdc.runtime.operators.schema.event.SchemaChangeResultResponse;
import com.ververica.cdc.runtime.operators.sink.SchemaEvolutionClient;
import com.ververica.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
    /** Storing route source table selector, sink table name in a tuple. */
    private transient List<Tuple2<Selectors, TableId>> routes;

    private transient TaskOperatorEventGateway toCoordinator;
    private transient SchemaEvolutionClient schemaEvolutionClient;
    private transient LoadingCache<TableId, Schema> originalSchema;
    private transient LoadingCache<TableId, Schema> evolvedSchema;
    private transient LoadingCache<TableId, Boolean> schemaDivergesMap;

    private final long rpcTimeOutInMillis;
    private final SchemaChangeBehavior schemaChangeBehavior;
    private transient int subTaskId;
    private final Tuple2<String, SupportedMetadataColumn> opTsMetaColumnName;

    @VisibleForTesting
    public SchemaOperator(Duration rpcTimeOut, List<Tuple2<String, TableId>> routingRules) {
        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.rpcTimeOutInMillis = rpcTimeOut.toMillis();
        this.schemaChangeBehavior = SchemaChangeBehavior.EVOLVE;
        this.routingRules = routingRules;
        this.opTsMetaColumnName = null;
    }

    public SchemaOperator(
            Duration rpcTimeOut,
            SchemaChangeBehavior schemaChangeBehavior,
            List<Tuple2<String, TableId>> routingRules,
            @Nullable Tuple2<String, SupportedMetadataColumn> opTsMetaColumnName) {
        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.rpcTimeOutInMillis = rpcTimeOut.toMillis();
        this.schemaChangeBehavior = schemaChangeBehavior;
        this.routingRules = routingRules;
        this.opTsMetaColumnName = opTsMetaColumnName;
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
        evolvedSchema =
                CacheBuilder.newBuilder()
                        .expireAfterAccess(CACHE_EXPIRE_DURATION)
                        .build(
                                new CacheLoader<TableId, Schema>() {
                                    @Override
                                    public Schema load(TableId tableId) {
                                        return getLatestEvolvedSchema(tableId);
                                    }
                                });
        originalSchema =
                CacheBuilder.newBuilder()
                        .expireAfterAccess(CACHE_EXPIRE_DURATION)
                        .build(
                                new CacheLoader<TableId, Schema>() {
                                    @Override
                                    public Schema load(TableId tableId) throws Exception {
                                        return getLatestOriginalSchema(tableId);
                                    }
                                });
        schemaDivergesMap =
                CacheBuilder.newBuilder()
                        .expireAfterAccess(CACHE_EXPIRE_DURATION)
                        .build(
                                new CacheLoader<TableId, Boolean>() {
                                    @Override
                                    public Boolean load(TableId tableId) throws Exception {
                                        return checkSchemaDiverges(tableId);
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
            processSchemaChangeEvents((SchemaChangeEvent) event);
        } else if (event instanceof DataChangeEvent) {
            processDataChangeEvents(streamRecord, (DataChangeEvent) event);
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
        // Update caches
        originalSchema.put(tableId, getLatestOriginalSchema(tableId));
        schemaDivergesMap.put(tableId, checkSchemaDiverges(tableId));
        getRoutedTable(tableId)
                .ifPresent(routed -> evolvedSchema.put(routed, getLatestEvolvedSchema(routed)));
    }

    private void processDataChangeEvents(StreamRecord<Event> streamRecord, DataChangeEvent event) {
        TableId tableId = event.tableId();
        Optional<TableId> optionalRoutedTable = getRoutedTable(tableId);
        if (optionalRoutedTable.isPresent()) {
            output.collect(
                    new StreamRecord<>(
                            normalizeSchemaChangeEvents(event, optionalRoutedTable.get(), false)));
        } else if (Boolean.FALSE.equals(schemaDivergesMap.getIfPresent(tableId))) {
            output.collect(new StreamRecord<>(normalizeSchemaChangeEvents(event, true)));
        } else if (opTsMetaColumnName != null) {
            output.collect(new StreamRecord<>(normalizeSchemaChangeEvents(event, false)));
        } else {
            output.collect(streamRecord);
        }
    }

    private DataChangeEvent normalizeSchemaChangeEvents(
            DataChangeEvent event, boolean tolerantMode) {
        return normalizeSchemaChangeEvents(event, event.tableId(), tolerantMode);
    }

    private DataChangeEvent normalizeSchemaChangeEvents(
            DataChangeEvent event, TableId renamedTableId, boolean tolerantMode) {
        try {
            Schema originalSchema = this.originalSchema.get(event.tableId());
            Schema evolvedTableSchema = evolvedSchema.get(renamedTableId);
            if (opTsMetaColumnName == null && originalSchema.equals(evolvedTableSchema)) {
                return ChangeEventUtils.recreateDataChangeEvent(event, renamedTableId);
            }
            switch (event.op()) {
                case INSERT:
                    return DataChangeEvent.insertEvent(
                            renamedTableId,
                            regenerateRecordData(
                                    event.after(),
                                    originalSchema,
                                    evolvedTableSchema,
                                    tolerantMode,
                                    event.meta()),
                            event.meta());
                case UPDATE:
                    return DataChangeEvent.updateEvent(
                            renamedTableId,
                            regenerateRecordData(
                                    event.before(),
                                    originalSchema,
                                    evolvedTableSchema,
                                    tolerantMode,
                                    Collections.emptyMap()),
                            regenerateRecordData(
                                    event.after(),
                                    originalSchema,
                                    evolvedTableSchema,
                                    tolerantMode,
                                    event.meta()),
                            event.meta());
                case DELETE:
                    return DataChangeEvent.deleteEvent(
                            renamedTableId,
                            regenerateRecordData(
                                    event.before(),
                                    originalSchema,
                                    evolvedTableSchema,
                                    tolerantMode,
                                    Collections.emptyMap()),
                            event.meta());
                case REPLACE:
                    return DataChangeEvent.replaceEvent(
                            renamedTableId,
                            regenerateRecordData(
                                    event.after(),
                                    originalSchema,
                                    evolvedTableSchema,
                                    tolerantMode,
                                    event.meta()),
                            event.meta());
                default:
                    throw new IllegalArgumentException(
                            String.format("Unrecognized operation type \"%s\"", event.op()));
            }
        } catch (Exception e) {
            throw new IllegalStateException("Unable to fill null for empty columns", e);
        }
    }

    private RecordData regenerateRecordData(
            RecordData recordData,
            Schema originalSchema,
            Schema routedTableSchema,
            boolean tolerantMode,
            Map<String, String> meta) {
        // Regenerate record data
        List<RecordData.FieldGetter> fieldGetters = new ArrayList<>();
        for (Column column : routedTableSchema.getColumns()) {
            String columnName = column.getName();
            int columnIndex = originalSchema.getColumnNames().indexOf(columnName);
            if (columnIndex == -1) {
                fieldGetters.add(new NullFieldGetter());
            } else {
                if (opTsMetaColumnName != null && columnName.equals(opTsMetaColumnName.f0)) {
                    fieldGetters.add(new OpTsFieldGetter(opTsMetaColumnName.f1, meta));
                    continue;
                }
                RecordData.FieldGetter fieldGetter =
                        RecordData.createFieldGetter(
                                originalSchema.getColumn(columnName).get().getType(), columnIndex);
                // Check type compatibility, ignoring nullability
                if (originalSchema
                        .getColumn(columnName)
                        .get()
                        .getType()
                        .nullable()
                        .equals(column.getType().nullable())) {
                    fieldGetters.add(fieldGetter);
                } else {
                    fieldGetters.add(
                            new TypeCoercionFieldGetter(
                                    column.getType(), fieldGetter, tolerantMode));
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
        if (opTsMetaColumnName != null) {
            // recreate SchemaChangeEvent to add op_ts column
            if (schemaChangeEvent instanceof CreateTableEvent) {
                try {
                    // Check if the table has been cached in master and contains the op_ts column
                    if (!getLatestOriginalSchema(tableId)
                            .getColumnNames()
                            .contains(opTsMetaColumnName.f0)) {
                        // The table has been cached in master, and does not have the op_ts column,
                        // so we only need to send an AddColumnEvent instead of CreateTableEvent to
                        // add the column
                        LOG.info(
                                "Table {} without column {} has been created in master, send AddColumnEvent to master",
                                tableId,
                                opTsMetaColumnName.f0);
                        schemaChangeEvent =
                                new AddColumnEvent(
                                        tableId,
                                        Collections.singletonList(
                                                new AddColumnEvent.ColumnWithPosition(
                                                        Column.physicalColumn(
                                                                opTsMetaColumnName.f0,
                                                                opTsMetaColumnName.f1.getType()))));
                    } else {
                        // The table has been cached in master, and has the op_ts column, so we
                        // don't need to transform the original CreateTableEvent, because it will
                        // end up being ignored by master
                        LOG.info(
                                "Table {} with column {} has been created in master",
                                tableId,
                                opTsMetaColumnName.f0);
                    }
                } catch (IllegalStateException e) {
                    // The table has not been cached in master, so we need to transform the original
                    // CreateTableEvent to add op_ts column
                    LOG.info(
                            "Table {} has not been created in master, send CreateTableEvent to master",
                            tableId);
                    schemaChangeEvent =
                            transformSchemaChangeEventWithOpTsColumn(tableId, schemaChangeEvent);
                    LOG.info(
                            "recreate SchemaChangeEvent to add op_ts column: {}",
                            schemaChangeEvent);
                }
            } else {
                schemaChangeEvent =
                        transformSchemaChangeEventWithOpTsColumn(tableId, schemaChangeEvent);
                LOG.info("recreate SchemaChangeEvent to add op_ts column: {}", schemaChangeEvent);
            }
        }
        // The request will block if another schema change event is being handled
        SchemaChangeResponse response = requestSchemaChange(tableId, schemaChangeEvent);
        if (response.isAccepted()) {
            LOG.info("{}> Sending the FlushEvent for table {}.", subTaskId, tableId);
            TableId sinkTable = response.getSchemaChangeEvents().get(0).tableId();
            output.collect(
                    new StreamRecord<>(
                            new FlushEvent(
                                    sinkTable,
                                    schemaChangeEvent.getType()
                                            == SchemaChangeEventType.CREATE_TABLE)));

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

    private SchemaChangeEvent transformSchemaChangeEventWithOpTsColumn(
            TableId tableId, SchemaChangeEvent event) {
        if (event instanceof CreateTableEvent) {
            Schema originalSchema = ((CreateTableEvent) event).getSchema();
            List<Column> columns = originalSchema.getColumns();
            if (checkColumnExists(columns, opTsMetaColumnName.f0)) {
                LOG.error(
                        "Metadata column name collision: table {} already has op_ts column {}",
                        tableId,
                        opTsMetaColumnName);
                throw new IllegalStateException(
                        "Metadata column name collision: table "
                                + tableId
                                + " already has op_ts column "
                                + opTsMetaColumnName.f0);
            }
            columns.add(
                    Column.physicalColumn(opTsMetaColumnName.f0, opTsMetaColumnName.f1.getType()));
            Schema newSchema = originalSchema.copy(columns);
            return new CreateTableEvent(tableId, newSchema);
        } else {
            if (event instanceof AddColumnEvent) {
                List<Column> addColumns =
                        ((AddColumnEvent) event)
                                .getAddedColumns().stream()
                                        .map(AddColumnEvent.ColumnWithPosition::getAddColumn)
                                        .collect(Collectors.toList());
                if (checkColumnExists(addColumns, opTsMetaColumnName.f0)) {
                    // When the op_ts metadata column is set, the source table cannot add a column
                    // with the same name
                    LOG.error(
                            "Could not add columns {}, because table {} already has op_ts metadata column {}",
                            addColumns,
                            tableId,
                            opTsMetaColumnName);
                    throw new IllegalStateException(
                            "Added column names conflict with metadata column names: table "
                                    + tableId
                                    + " already has op_ts column "
                                    + opTsMetaColumnName.f0);
                }
            }
            try {
                List<Column> columnsWithoutOpTs =
                        originalSchema.get(tableId).getColumns().stream()
                                .filter(col -> !col.getName().equals(opTsMetaColumnName.f0))
                                .collect(Collectors.toList());
                // Transform the AddColumnEvent to add the column after the last column of the
                // source table
                Optional<SchemaChangeEvent> schemaChangeEvent =
                        SchemaUtils.transformSchemaChangeEvent(true, columnsWithoutOpTs, event);
                if (schemaChangeEvent.isPresent()) {
                    return schemaChangeEvent.get();
                } else {
                    throw new IllegalStateException(
                            "Unexpected schema change event with OpTs: " + event);
                }
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private boolean checkColumnExists(List<Column> columns, String columnName) {
        return columns.stream().anyMatch(column -> column.getName().equals(columnName));
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

    private Schema getLatestEvolvedSchema(TableId tableId) {
        try {
            Optional<Schema> optionalSchema = schemaEvolutionClient.getLatestEvolvedSchema(tableId);
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

    private Schema getLatestOriginalSchema(TableId tableId) {
        try {
            Optional<Schema> optionalSchema =
                    schemaEvolutionClient.getLatestOriginalSchema(tableId);
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

    private Boolean checkSchemaDiverges(TableId tableId) {
        try {
            return getLatestEvolvedSchema(tableId).equals(getLatestOriginalSchema(tableId));
        } catch (IllegalStateException e) {
            // schema fetch failed, regard it as diverged
            return true;
        }
    }

    private static class OpTsFieldGetter implements RecordData.FieldGetter {
        private final Object opTsValue;

        public OpTsFieldGetter(
                SupportedMetadataColumn supportedMetadataColumn, Map<String, String> meta) {
            if (meta.isEmpty()) {
                opTsValue = null;
            } else {
                opTsValue = supportedMetadataColumn.read(meta);
            }
        }

        @Override
        public Object getFieldOrNull(RecordData recordData) {
            return opTsValue;
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
        private final boolean tolerantMode;

        public TypeCoercionFieldGetter(
                DataType destinationType,
                RecordData.FieldGetter originalFieldGetter,
                boolean tolerantMode) {
            this.destinationType = destinationType;
            this.originalFieldGetter = originalFieldGetter;
            this.tolerantMode = tolerantMode;
        }

        private Object fail(IllegalArgumentException e) throws IllegalArgumentException {
            if (tolerantMode) {
                return null;
            }
            throw e;
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
                    return fail(
                            new IllegalArgumentException(
                                    String.format(
                                            "Cannot fit type \"%s\" into a BIGINT column. "
                                                    + "Currently only TINYINT / SMALLINT / INT can be accepted by a BIGINT column",
                                            originalField.getClass())));
                }
            } else if (destinationType.is(DataTypeFamily.APPROXIMATE_NUMERIC)) {
                if (originalField instanceof Float) {
                    // FLOAT
                    return ((Float) originalField).doubleValue();
                } else {
                    return fail(
                            new IllegalArgumentException(
                                    String.format(
                                            "Cannot fit type \"%s\" into a DOUBLE column. "
                                                    + "Currently only FLOAT can be accepted by a DOUBLE column",
                                            originalField.getClass())));
                }
            } else if (destinationType.is(DataTypeRoot.VARCHAR)) {
                if (originalField instanceof StringData) {
                    return originalField;
                } else {
                    return fail(
                            new IllegalArgumentException(
                                    String.format(
                                            "Cannot fit type \"%s\" into a STRING column. "
                                                    + "Currently only CHAR / VARCHAR can be accepted by a STRING column",
                                            originalField.getClass())));
                }
            } else {
                return fail(
                        new IllegalArgumentException(
                                String.format(
                                        "Column type \"%s\" doesn't support type coercion",
                                        destinationType)));
            }
        }
    }
}
