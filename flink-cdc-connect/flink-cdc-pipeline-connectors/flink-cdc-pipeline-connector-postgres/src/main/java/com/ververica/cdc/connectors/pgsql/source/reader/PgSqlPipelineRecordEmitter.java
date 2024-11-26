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
import com.ververica.cdc.common.schema.Selectors;
import com.ververica.cdc.connectors.base.source.meta.offset.OffsetFactory;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitState;
import com.ververica.cdc.connectors.base.source.reader.IncrementalSourceRecordEmitter;
import com.ververica.cdc.connectors.pgsql.utils.PostgresSchemaUtils;
import com.ververica.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import org.apache.kafka.connect.source.SourceRecord;

import javax.annotation.Nullable;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
            List<String> tableList = sourceConfig.getTableList();
            String database = sourceConfig.getDatabaseList().get(0);
            tableList =
                    tableList.stream().map(e -> database + "." + e).collect(Collectors.toList());
            Selectors selectors =
                    new Selectors.SelectorsBuilder()
                            .includeTables(String.join(", ", tableList))
                            .build();
            String[] capturedTables = getTableList(sourceConfig, selectors);
            List<TableId> capturedTableIds = new ArrayList<>();
            for (String table : capturedTables) {
                TableId capturedTableId = TableId.parse(table);
                capturedTableIds.add(capturedTableId);
            }
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

    private static String[] getTableList(PostgresSourceConfig sourceConfig, Selectors selectors) {
        return listTables(sourceConfig, null).stream()
                .filter(selectors::isMatch)
                .map(com.ververica.cdc.common.event.TableId::toString)
                .toArray(String[]::new);
    }

    public static List<com.ververica.cdc.common.event.TableId> listTables(
            PostgresSourceConfig sourceConfig, @Nullable String dbName) {
        try (JdbcConnection jdbc = PostgresSchemaUtils.createPostgresConnection(sourceConfig)) {
            List<String> databases =
                    dbName != null
                            ? Collections.singletonList(dbName)
                            : PostgresSchemaUtils.listDatabases(jdbc);

            List<com.ververica.cdc.common.event.TableId> tableIds = new ArrayList<>();
            for (String database : databases) {
                tableIds.addAll(PostgresSchemaUtils.listTables(jdbc, database));
            }
            return tableIds;
        } catch (SQLException e) {
            throw new RuntimeException("Error to list databases: " + e.getMessage(), e);
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
