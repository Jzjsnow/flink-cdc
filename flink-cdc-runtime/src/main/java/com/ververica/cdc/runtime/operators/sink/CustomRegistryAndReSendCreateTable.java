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

package com.ververica.cdc.runtime.operators.sink;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import com.ververica.cdc.common.event.ChangeEvent;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.FlushEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Schema;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/** CustomRegistryAndReSendCreateTable. */
public class CustomRegistryAndReSendCreateTable extends AbstractStreamOperator<Event>
        implements OneInputStreamOperator<Event, Event>, BoundedOneInput {
    private SchemaEvolutionClient schemaEvolutionClient;

    private final OperatorID schemaOperatorID;

    /** A set of {@link TableId} that already processed {@link CreateTableEvent}. */
    private final Set<TableId> processedTableIds;

    public CustomRegistryAndReSendCreateTable(OperatorID schemaOperatorID) {
        this.schemaOperatorID = schemaOperatorID;
        this.processedTableIds = new HashSet<>();
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<Event>> output) {
        super.setup(containingTask, config, output);
        schemaEvolutionClient =
                new SchemaEvolutionClient(
                        containingTask.getEnvironment().getOperatorCoordinatorEventGateway(),
                        schemaOperatorID);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        // TODO!!!
        // how would this happen when parallelism of Iceberg Writer did not match this operator?
        // or in the first version, we need user to set the parallelism of this two to the same
        schemaEvolutionClient.registerSubtask(getRuntimeContext().getIndexOfThisSubtask());
    }

    @Override
    public void processElement(StreamRecord<Event> element) throws Exception {
        Event event = element.getValue();

        // FlushEvent triggers flush
        if (event instanceof FlushEvent) {
            handleFlushEvent((FlushEvent) event);
            return;
        }

        // CreateTableEvent marks the table as processed directly
        if (event instanceof CreateTableEvent) {
            processedTableIds.add(((CreateTableEvent) event).tableId());
            output.collect(new StreamRecord<>(event));
            return;
        }

        // Check if the table is processed before emitting all other events, because we have to make
        // sure that sink have a view of the full schema before processing any change events,
        // including schema changes.
        ChangeEvent changeEvent = (ChangeEvent) event;
        TableId tableId = changeEvent.tableId();
        if (!processedTableIds.contains(tableId)) {
            emitLatestSchema(changeEvent.tableId());
            processedTableIds.add(changeEvent.tableId());
        }
        processedTableIds.add(changeEvent.tableId());
        output.collect(element);
    }

    @Override
    public void endInput() throws Exception {}

    private void handleFlushEvent(FlushEvent flushEvent) throws Exception {
        // do not need to flush element to iceberg
        if (!processedTableIds.contains(flushEvent.getTableId())
                && !flushEvent.getIsForCreateTableEvent()) {
            LOG.info("Table {} has not been processed", flushEvent.getTableId());
            emitLatestSchema(flushEvent.getTableId());
            processedTableIds.add(flushEvent.getTableId());
        }
        // only need to notify schema registry
        schemaEvolutionClient.notifyFlushSuccess(
                getRuntimeContext().getIndexOfThisSubtask(), flushEvent.getTableId());
    }

    public void emitLatestSchema(TableId tableId) throws Exception {
        Optional<Schema> schema = schemaEvolutionClient.getLatestEvolvedSchema(tableId);
        if (schema.isPresent()) {
            output.collect(new StreamRecord<>(new CreateTableEvent(tableId, schema.get())));
        } else {
            throw new RuntimeException(
                    "Could not find schema message from SchemaRegistry for " + tableId);
        }
        processedTableIds.add(tableId);
    }
}
