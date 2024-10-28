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
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.util.SerializedValue;

import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.runtime.operators.schema.SchemaOperator;
import com.ververica.cdc.runtime.operators.schema.coordinator.SchemaRegistry;
import com.ververica.cdc.runtime.operators.schema.event.FlushSuccessEvent;
import com.ververica.cdc.runtime.operators.schema.event.GetEvolvedSchemaRequest;
import com.ververica.cdc.runtime.operators.schema.event.GetEvolvedSchemaResponse;
import com.ververica.cdc.runtime.operators.schema.event.GetOriginalSchemaRequest;
import com.ververica.cdc.runtime.operators.schema.event.GetOriginalSchemaResponse;
import com.ververica.cdc.runtime.operators.schema.event.SinkWriterRegisterEvent;

import java.io.IOException;
import java.util.Optional;

import static com.ververica.cdc.runtime.operators.schema.event.CoordinationResponseUtils.unwrap;

/**
 * Client for {@link DataSinkWriterOperator} interact with {@link SchemaRegistry} when table schema
 * evolution happened.
 */
public class SchemaEvolutionClient {

    private final TaskOperatorEventGateway toCoordinator;

    /** a determinant OperatorID of {@link SchemaOperator}. */
    private final OperatorID schemaOperatorID;

    public SchemaEvolutionClient(
            TaskOperatorEventGateway toCoordinator, OperatorID schemaOperatorID) {
        this.toCoordinator = toCoordinator;
        this.schemaOperatorID = schemaOperatorID;
    }

    /** send {@link SinkWriterRegisterEvent} to {@link SchemaRegistry}. */
    public void registerSubtask(int subtask) throws IOException {
        toCoordinator.sendOperatorEventToCoordinator(
                schemaOperatorID, new SerializedValue<>(new SinkWriterRegisterEvent(subtask)));
    }

    /** send {@link FlushSuccessEvent} to {@link SchemaRegistry}. */
    public void notifyFlushSuccess(int subtask, TableId tableId) throws IOException {
        toCoordinator.sendOperatorEventToCoordinator(
                schemaOperatorID, new SerializedValue<>(new FlushSuccessEvent(subtask, tableId)));
    }

    public Optional<Schema> getLatestEvolvedSchema(TableId tableId) throws Exception {
        GetEvolvedSchemaResponse getEvolvedSchemaResponse =
                unwrap(
                        toCoordinator
                                .sendRequestToCoordinator(
                                        schemaOperatorID,
                                        new SerializedValue<>(
                                                GetEvolvedSchemaRequest.ofLatestSchema(tableId)))
                                .get());
        return getEvolvedSchemaResponse.getSchema();
    }

    public Optional<Schema> getLatestOriginalSchema(TableId tableId) throws Exception {
        GetOriginalSchemaResponse getOriginalSchemaResponse =
                unwrap(
                        toCoordinator
                                .sendRequestToCoordinator(
                                        schemaOperatorID,
                                        new SerializedValue<>(
                                                GetOriginalSchemaRequest.ofLatestSchema(tableId)))
                                .get());
        return getOriginalSchemaResponse.getSchema();
    }
}
