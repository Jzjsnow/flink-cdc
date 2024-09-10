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
import com.ververica.cdc.runtime.operators.sink.SchemaEvolutionClient;

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
            handleFlushEvent(((FlushEvent) event));
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
            Optional<Schema> schema = schemaEvolutionClient.getLatestSchema(tableId);
            if (schema.isPresent()) {
                output.collect(new StreamRecord<>(new CreateTableEvent(tableId, schema.get())));
            } else {
                throw new RuntimeException(
                        "Could not find schema message from SchemaRegistry for " + tableId);
            }
            processedTableIds.add(changeEvent.tableId());
        }

        output.collect(element);
    }

    @Override
    public void endInput() throws Exception {}

    private void handleFlushEvent(FlushEvent flushEvent) throws Exception {
        // do not need to flush element to iceberg
        // only need to notify schema registry
        schemaEvolutionClient.notifyFlushSuccess(
                getRuntimeContext().getIndexOfThisSubtask(), flushEvent.getTableId());
    }
}
