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

package com.ververica.cdc.connectors.pgsql.source.reader;

import com.ververica.cdc.common.annotation.PublicEvolving;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.connectors.pgsql.utils.PostgresSchemaUtils;
import com.ververica.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.Validator;
import com.ververica.cdc.debezium.internal.DebeziumOffset;
import com.ververica.cdc.debezium.internal.Handover;
import io.debezium.engine.DebeziumEngine;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;

import javax.annotation.Nullable;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static com.ververica.cdc.connectors.pgsql.utils.PostgresSchemaUtils.getCapturedTableIds;

/**
 * The {@link PgsqlDebeziumSourceFunction} is a streaming data source that pulls captured change
 * data from databases into Flink.
 *
 * <p>There are two workers during the runtime. One worker periodically pulls records from the
 * database and pushes the records into the {@link Handover}. The other worker consumes the records
 * from the {@link Handover} and convert the records to the data in Flink style. The reason why
 * don't use one workers is because debezium has different behaviours in snapshot phase and
 * streaming phase.
 *
 * <p>Here we use the {@link Handover} as the buffer to submit data from the producer to the
 * consumer. Because the two threads don't communicate to each other directly, the error reporting
 * also relies on {@link Handover}. When the engine gets errors, the engine uses the {@link
 * DebeziumEngine.CompletionCallback} to report errors to the {@link Handover} and wakes up the
 * consumer to check the error. However, the source function just closes the engine and wakes up the
 * producer if the error is from the Flink side.
 *
 * <p>If the execution is canceled or finish(only snapshot phase), the exit logic is as same as the
 * logic in the error reporting.
 *
 * <p>The source function participates in checkpointing and guarantees that no data is lost during a
 * failure, and that the computation processes elements "exactly once".
 *
 * <p>Note: currently, the source function can't run in multiple parallel instances.
 *
 * <p>Please refer to Debezium's documentation for the available configuration properties:
 * https://debezium.io/documentation/reference/1.9/development/engine.html#engine-properties
 */
@PublicEvolving
public class PgsqlDebeziumSourceFunction<T> extends DebeziumSourceFunction<T> {
    private Set<TableId> alreadySendCreateTableTables;
    private String[] tableList;
    private PostgresSourceConfig sourceConfig;

    public PgsqlDebeziumSourceFunction(
            DebeziumDeserializationSchema<T> deserializer,
            Properties properties,
            @Nullable DebeziumOffset specificOffset,
            Validator validator,
            String[] tableList,
            PostgresSourceConfig sourceConfig) {
        super(deserializer, properties, specificOffset, validator);
        this.tableList = tableList;
        this.sourceConfig = sourceConfig;
        alreadySendCreateTableTables = new HashSet<>();
    }

    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {
        try (JdbcConnection jdbc = PostgresSchemaUtils.createPostgresConnection(sourceConfig)) {
            List<TableId> capturedTableIds = getCapturedTableIds(sourceConfig);
            for (TableId tableId : capturedTableIds) {
                if (!alreadySendCreateTableTables.contains(tableId)) {
                    try {
                        sendCreateTableEvent(jdbc, tableId, (SourceContext<Event>) sourceContext);
                        alreadySendCreateTableTables.add(tableId);
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Cannot start emitter to fetch table schema.", e);
        }
        super.run(sourceContext);
    }

    private void sendCreateTableEvent(
            JdbcConnection jdbc, TableId tableId, SourceContext<Event> sourceContext) {
        Schema schema = PostgresSchemaUtils.getSchema(jdbc, tableId);
        sourceContext.collect(
                new CreateTableEvent(
                        com.ververica.cdc.common.event.TableId.tableId(
                                tableId.schema(), tableId.table()),
                        schema));
    }
}
