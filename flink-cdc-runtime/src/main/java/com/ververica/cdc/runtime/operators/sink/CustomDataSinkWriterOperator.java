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

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.event.ChangeEvent;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.FlushEvent;
import com.ververica.cdc.common.event.TableId;

import java.util.HashSet;
import java.util.Set;

/**
 * An operator that processes records to be written into a {@link Sink}.
 *
 * <p>The operator is a proxy of SinkWriterOperator in Flink.
 *
 * <p>The operator is always part of a sink pipeline and is the first operator.
 *
 * @param <CommT> the type of the committable (to send to downstream operators)
 */
@Internal
public class CustomDataSinkWriterOperator<CommT> extends DataSinkWriterOperator<CommT> {

    /** A set of {@link TableId} that already processed {@link CreateTableEvent}. */
    private final Set<TableId> processedTableIds;

    public CustomDataSinkWriterOperator(
            Sink<Event> sink,
            ProcessingTimeService processingTimeService,
            MailboxExecutor mailboxExecutor,
            OperatorID schemaOperatorID) {
        super(sink, processingTimeService, mailboxExecutor, schemaOperatorID);
        this.processedTableIds = new HashSet<>();
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
            super.<OneInputStreamOperator<Event, CommittableMessage<CommT>>>getFlinkWriterOperator()
                    .processElement(element);
            return;
        }

        // Check if the table is processed before emitting all other events, because we have to make
        // sure that sink have a view of the full schema before processing any change events,
        // including schema changes.
        ChangeEvent changeEvent = (ChangeEvent) event;
        if (!processedTableIds.contains(changeEvent.tableId())) {
            emitLatestSchema(changeEvent.tableId());
            processedTableIds.add(changeEvent.tableId());
        }
        processedTableIds.add(changeEvent.tableId());
        this.<OneInputStreamOperator<Event, CommittableMessage<CommT>>>getFlinkWriterOperator()
                .processElement(element);
    }

    // ----------------------------- Helper functions -------------------------------

    private void handleFlushEvent(FlushEvent event) throws Exception {
        //        copySinkWriter.flush(false);
        SchemaEvolutionClient schemaEvolutionClient = super.getSchemaEvolutionClient();
        schemaEvolutionClient.notifyFlushSuccess(
                getRuntimeContext().getIndexOfThisSubtask(), event.getTableId());
    }
}
