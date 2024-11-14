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

package com.ververica.cdc.runtime.operators.schema.coordinator;

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.common.event.SchemaChangeEventType;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.pipeline.SchemaChangeBehavior;
import com.ververica.cdc.common.sink.MetadataApplier;
import com.ververica.cdc.common.utils.Preconditions;
import com.ververica.cdc.runtime.operators.schema.event.SchemaChangeProcessingResponse;
import com.ververica.cdc.runtime.operators.schema.event.SchemaChangeRequest;
import com.ververica.cdc.runtime.operators.schema.event.SchemaChangeResponse;
import com.ververica.cdc.runtime.operators.schema.event.SchemaChangeResultResponse;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static com.ververica.cdc.runtime.operators.schema.event.CoordinationResponseUtils.wrap;

/** A handler to deal with all requests and events for {@link SchemaRegistry}. */
@Internal
public class SchemaRegistryRequestHandler implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryRequestHandler.class);

    /** The {@link MetadataApplier} for every table. */
    private final MetadataApplier metadataApplier;
    /** All active sink writers. */
    private final Set<Integer> activeSinkWriters;
    /** Schema manager holding schema for all tables. */
    private final SchemaManager schemaManager;
    private final SchemaDerivation schemaDerivation;

    /**
     * Atomic flag indicating if current RequestHandler could accept more schema changes for now.
     */
    private final AtomicReference<RequestStatus> schemaChangeStatus;

    private volatile Throwable currentChangeException;
    private volatile List<SchemaChangeEvent> currentDerivedSchemaChangeEvents;
    private volatile List<SchemaChangeEvent> currentFinishedSchemaChanges;
    private volatile List<SchemaChangeEvent> currentIgnoredSchemaChanges;

    /** Sink writers which have sent flush success events for the request. */
    private final Set<Integer> flushedSinkWriters;

    /** Executor service to execute schema change. */
    private final ExecutorService schemaChangeThreadPool;

    private final SchemaChangeBehavior schemaChangeBehavior;

    public SchemaRegistryRequestHandler(
            MetadataApplier metadataApplier,
            SchemaManager schemaManager,
            SchemaChangeBehavior schemaChangeBehavior,
            SchemaDerivation schemaDerivation) {
        this.metadataApplier = metadataApplier;
        this.schemaManager = schemaManager;
        this.schemaChangeBehavior = schemaChangeBehavior;
        this.schemaDerivation = schemaDerivation;

        this.activeSinkWriters = ConcurrentHashMap.newKeySet();
        this.flushedSinkWriters = ConcurrentHashMap.newKeySet();
        this.schemaChangeThreadPool = Executors.newSingleThreadExecutor();

        this.currentDerivedSchemaChangeEvents = new ArrayList<>();
        this.currentFinishedSchemaChanges = new ArrayList<>();
        this.currentIgnoredSchemaChanges = new ArrayList<>();
        this.schemaChangeStatus = new AtomicReference<>(RequestStatus.IDLE);
    }

    /**
     * Apply the schema change to the external system.
     *
     * @param tableId the table need to change schema
     * @param derivedSchemaChangeEvents list of the schema changes
     */
    private void applySchemaChange(
            TableId tableId, List<SchemaChangeEvent> derivedSchemaChangeEvents) {
        for (SchemaChangeEvent changeEvent : derivedSchemaChangeEvents) {
            if (changeEvent.getType() != SchemaChangeEventType.CREATE_TABLE) {
                if (schemaChangeBehavior == SchemaChangeBehavior.IGNORE) {
                    currentIgnoredSchemaChanges.add(changeEvent);
                    continue;
                }
            }
            if (!metadataApplier.acceptsSchemaEvolutionType(changeEvent.getType())) {
                LOG.info("Ignored schema change {} to table {}.", changeEvent, tableId);
                currentIgnoredSchemaChanges.add(changeEvent);
            } else {
                try {
                    metadataApplier.applySchemaChange(changeEvent);
                    LOG.info("Applied schema change {} to table {}.", changeEvent, tableId);
                    currentFinishedSchemaChanges.add(changeEvent);
                } catch (Throwable t) {
                    LOG.error(
                            "Failed to apply schema change {} to table {}. Caused by: {}",
                            changeEvent,
                            tableId,
                            t);
                    currentChangeException = t;
                    break;
                }
            }
        }
        Preconditions.checkState(
                schemaChangeStatus.compareAndSet(RequestStatus.APPLYING, RequestStatus.FINISHED),
                "Illegal schemaChangeStatus state: should be APPLYING before applySchemaChange finishes, not "
                        + schemaChangeStatus.get());
        LOG.info(
                "SchemaChangeStatus switched from APPLYING to FINISHED for request {}.",
                currentDerivedSchemaChangeEvents);
    }

    /**
     * Handle the {@link SchemaChangeRequest} and wait for all sink subtasks flushing.
     *
     * @param request the received SchemaChangeRequest
     */
    public CompletableFuture<CoordinationResponse> handleSchemaChangeRequest(
            SchemaChangeRequest request) {
        if (schemaChangeStatus.compareAndSet(RequestStatus.IDLE, RequestStatus.WAITING_FOR_FLUSH)) {
            LOG.info(
                    "Received schema change event request {} from table {}. SchemaChangeStatus switched from IDLE to WAITING_FOR_FLUSH, other requests will be blocked.",
                    request.getSchemaChangeEvent(),
                    request.getTableId().toString());
            SchemaChangeEvent event = request.getSchemaChangeEvent();

            // If this schema change event has been requested by another subTask, ignore it.
            if (schemaManager.isOriginalSchemaChangeEventRedundant(event)) {
                LOG.info("Event {} has been addressed before, ignoring it.", event);
                clearCurrentSchemaChangeRequest();
                Preconditions.checkState(
                        schemaChangeStatus.compareAndSet(
                                RequestStatus.WAITING_FOR_FLUSH, RequestStatus.IDLE),
                        "Illegal schemaChangeStatus state: should still in WAITING_FOR_FLUSH state if event was duplicated, not "
                                + schemaChangeStatus.get());
                LOG.info(
                        "SchemaChangeStatus switched from WAITING_FOR_FLUSH to IDLE for request {} due to duplicated request.",
                        request);
                return CompletableFuture.completedFuture(wrap(SchemaChangeResponse.duplicate()));
            }
            schemaManager.applySchemaChange(event);
            List<SchemaChangeEvent> derivedSchemaChangeEvents =
                    schemaDerivation.applySchemaChange(request.getSchemaChangeEvent());

            // If this schema change event is filtered out by LENIENT mode or merging table route
            // strategies, ignore it.
            if (derivedSchemaChangeEvents.isEmpty()) {
                LOG.info("Event {} is omitted from sending to downstream, ignoring it.", event);
                clearCurrentSchemaChangeRequest();
                Preconditions.checkState(
                        schemaChangeStatus.compareAndSet(
                                RequestStatus.WAITING_FOR_FLUSH, RequestStatus.IDLE),
                        "Illegal schemaChangeStatus state: should still in WAITING_FOR_FLUSH state if event was ignored.");
                return CompletableFuture.completedFuture(wrap(SchemaChangeResponse.ignored()));
            }
            currentDerivedSchemaChangeEvents = new ArrayList<>(derivedSchemaChangeEvents);

            return CompletableFuture.completedFuture(
                    wrap(SchemaChangeResponse.accepted(derivedSchemaChangeEvents)));
        } else {
            LOG.info(
                    "Schema Registry is busy processing a schema change request, could not handle request {} for now.",
                    request);
            return CompletableFuture.completedFuture(wrap(SchemaChangeResponse.busy()));
        }
    }

    /**
     * Register a sink subtask.
     *
     * @param sinkSubtask the sink subtask to register
     */
    public void registerSinkWriter(int sinkSubtask) {
        LOG.info("Register sink subtask {}.", sinkSubtask);
        activeSinkWriters.add(sinkSubtask);
    }

    /**
     * Record flushed sink subtasks after receiving FlushSuccessEvent.
     *
     * @param tableId the subtask in SchemaOperator and table that the FlushEvent is about
     * @param sinkSubtask the sink subtask succeed flushing
     */
    public void flushSuccess(TableId tableId, int sinkSubtask, int parallelism) {
        flushedSinkWriters.add(sinkSubtask);
        if (activeSinkWriters.size() < parallelism) {
            LOG.info(
                    "Not all active sink writers have been registered. Current {}, expected {}.",
                    activeSinkWriters.size(),
                    parallelism);
            return;
        }
        if (flushedSinkWriters.equals(activeSinkWriters)) {
            Preconditions.checkState(
                    schemaChangeStatus.compareAndSet(
                            RequestStatus.WAITING_FOR_FLUSH, RequestStatus.APPLYING),
                    "Illegal schemaChangeStatus state: should be WAITING_FOR_FLUSH before collecting enough FlushEvents, not "
                            + schemaChangeStatus);
            LOG.info(
                    "All sink subtask have flushed for table {}. Start to apply schema change.",
                    tableId.toString());
            schemaChangeThreadPool.submit(
                    () -> applySchemaChange(tableId, currentDerivedSchemaChangeEvents));
        }
    }

    public CompletableFuture<CoordinationResponse> getSchemaChangeResult() {
        Preconditions.checkState(
                !schemaChangeStatus.get().equals(RequestStatus.IDLE),
                "Illegal schemaChangeStatus: should not be IDLE before getting schema change request results.");
        if (schemaChangeStatus.compareAndSet(RequestStatus.FINISHED, RequestStatus.IDLE)) {
            LOG.info(
                    "SchemaChangeStatus switched from FINISHED to IDLE for request {}",
                    currentDerivedSchemaChangeEvents);
            // This request has been finished, return it and prepare for the next request
            List<SchemaChangeEvent> finishedEvents = clearCurrentSchemaChangeRequest();
            return CompletableFuture.supplyAsync(
                    () -> wrap(new SchemaChangeResultResponse(finishedEvents)));
        } else {
            // Still working on schema change request, waiting it
            return CompletableFuture.supplyAsync(() -> wrap(new SchemaChangeProcessingResponse()));
        }
    }

    @Override
    public void close() throws IOException {
        if (schemaChangeThreadPool != null) {
            schemaChangeThreadPool.shutdown();
        }
    }

    private List<SchemaChangeEvent> clearCurrentSchemaChangeRequest() {
        if (currentChangeException != null) {
            throw new RuntimeException("Failed to apply schema change.", currentChangeException);
        }
        List<SchemaChangeEvent> finishedSchemaChanges =
                new ArrayList<>(currentFinishedSchemaChanges);
        flushedSinkWriters.clear();
        currentDerivedSchemaChangeEvents.clear();
        currentFinishedSchemaChanges.clear();
        currentIgnoredSchemaChanges.clear();
        currentChangeException = null;
        return finishedSchemaChanges;
    }

    // Schema change event state could transfer in the following way:
    //
    //      -------- B --------
    //      |                 |
    //      v                 |
    //  --------           ---------------------
    //  | IDLE | --- A --> | WAITING_FOR_FLUSH |
    //  --------           ---------------------
    //     ^                        |
    //      E                       C
    //       \                      v
    //  ------------          ------------
    //  | FINISHED | <-- D -- | APPLYING |
    //  ------------          ------------
    //
    //  A: When a request came to an idling request handler.
    //  B: When current request is duplicate or ignored by LENIENT / routed table merging
    // strategies.
    //  C: When schema registry collected enough flush success events, and actually started to apply
    // schema changes.
    //  D: When schema change application finishes (successfully or with exceptions)
    //  E: When current schema change request result has been retrieved by SchemaOperator, and ready
    // for the next request.
    enum RequestStatus {
        IDLE,
        WAITING_FOR_FLUSH,
        APPLYING,
        FINISHED
    }
}
