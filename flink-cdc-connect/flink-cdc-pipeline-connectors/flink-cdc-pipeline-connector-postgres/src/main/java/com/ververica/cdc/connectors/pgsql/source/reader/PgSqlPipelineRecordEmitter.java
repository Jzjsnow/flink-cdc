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

import org.apache.flink.api.connector.source.SourceOutput;

import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.connectors.base.source.meta.offset.OffsetFactory;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitState;
import com.ververica.cdc.connectors.base.source.reader.IncrementalSourceRecordEmitter;
import com.ververica.cdc.connectors.pgsql.utils.PostgresSchemaUtils;
import com.ververica.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import org.apache.kafka.connect.source.SourceRecord;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.ververica.cdc.connectors.pgsql.utils.PostgresSchemaUtils.getCapturedTableIds;

/**
 * A builder to build a SourceFunction which can read snapshot and continue to consume binlog for
 * PostgreSQL.
 */
public class PgSqlPipelineRecordEmitter extends IncrementalSourceRecordEmitter<Event> {
    private Set<TableId> alreadySendCreateTableTables;
    private List<String> tableList;
    private PostgresSourceConfig sourceConfig;
    private final List<CreateTableEvent> createTableEventCache;
    private boolean alreadySendCreateTableForBinlogSplit = false;

    public PgSqlPipelineRecordEmitter(
            DebeziumDeserializationSchema<Event> debeziumDeserializationSchema,
            boolean includeSchemaChanges,
            OffsetFactory offsetFactory,
            PostgresSourceConfig sourceConfig) {
        super(debeziumDeserializationSchema, null, includeSchemaChanges, offsetFactory);
        this.tableList = sourceConfig.getTableList();
        this.sourceConfig = sourceConfig;
        alreadySendCreateTableTables = new HashSet<>();
        this.createTableEventCache = new ArrayList<>();
        try (JdbcConnection jdbc = PostgresSchemaUtils.createPostgresConnection(sourceConfig)) {
            List<TableId> capturedTableIds = getCapturedTableIds(sourceConfig);
            for (TableId tableId : capturedTableIds) {
                Schema schema = PostgresSchemaUtils.getSchema(jdbc, tableId);
                createTableEventCache.add(
                        new CreateTableEvent(
                                com.ververica.cdc.common.event.TableId.tableId(
                                        tableId.schema(), tableId.table()),
                                schema));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Cannot start emitter to fetch table schema.", e);
        }
    }

    @Override
    protected void processElement(
            SourceRecord element, SourceOutput<Event> output, SourceSplitState splitState)
            throws Exception {
        if (!alreadySendCreateTableForBinlogSplit) {
            createTableEventCache.forEach(output::collect);
            alreadySendCreateTableForBinlogSplit = true;
        }
        super.processElement(element, output, splitState);
    }
}
