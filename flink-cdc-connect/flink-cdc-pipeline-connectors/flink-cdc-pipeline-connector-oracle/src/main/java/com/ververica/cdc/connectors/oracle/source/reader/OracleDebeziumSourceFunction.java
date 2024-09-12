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

package com.ververica.cdc.connectors.oracle.source.reader;

import com.ververica.cdc.common.annotation.PublicEvolving;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.types.BigIntType;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.common.types.FloatType;
import com.ververica.cdc.common.types.IntType;
import com.ververica.cdc.common.types.LocalZonedTimestampType;
import com.ververica.cdc.common.types.TimestampType;
import com.ververica.cdc.common.types.VarCharType;
import com.ververica.cdc.common.types.ZonedTimestampType;
import com.ververica.cdc.connectors.oracle.dto.ColumnInfo;
import com.ververica.cdc.connectors.oracle.source.config.OracleSourceConfig;
import com.ververica.cdc.connectors.oracle.utils.DebeziumUtils;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;

/**
 * The {@link OracleDebeziumSourceFunction} is a streaming data source that pulls captured change
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
public class OracleDebeziumSourceFunction<T> extends DebeziumSourceFunction<T> {
    private Set<TableId> alreadySendCreateTableTables;
    private String[] tableList;
    private OracleSourceConfig sourceConfig;

    public OracleDebeziumSourceFunction(
            DebeziumDeserializationSchema<T> deserializer,
            Properties properties,
            @Nullable DebeziumOffset specificOffset,
            Validator validator,
            String[] tableList,
            OracleSourceConfig sourceConfig) {
        super(deserializer, properties, specificOffset, validator);
        this.tableList = tableList;
        this.sourceConfig = sourceConfig;
        alreadySendCreateTableTables = new HashSet<>();
    }

    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {

        Arrays.stream(tableList)
                .sequential()
                .forEach(
                        e -> {
                            TableId tableId = TableId.parse(e);
                            if (!alreadySendCreateTableTables.contains(tableId)) {
                                try (JdbcConnection jdbc =
                                        DebeziumUtils.createOracleConnection(sourceConfig)) {
                                    sendCreateTableEvent(
                                            jdbc, tableId, (SourceContext<Event>) sourceContext);
                                    alreadySendCreateTableTables.add(tableId);
                                } catch (Exception ex) {
                                    ex.printStackTrace();
                                }
                            }
                        });
        super.run(sourceContext);
    }

    private void sendCreateTableEvent(
            JdbcConnection jdbc, TableId tableId, SourceContext<Event> sourceContext) {
        Schema schema = getSchema(jdbc, tableId);
        sourceContext.collect(
                new CreateTableEvent(
                        com.ververica.cdc.common.event.TableId.tableId(
                                tableId.catalog(), tableId.table()),
                        schema));
    }

    private Schema getSchema(JdbcConnection jdbc, TableId tableId) {
        List<ColumnInfo> columns = showCreateTable(jdbc, tableId);
        List<String> pks = getTablePks(jdbc, tableId);
        List<com.ververica.cdc.common.schema.Column> list = new ArrayList<>();
        for (ColumnInfo columnInfo : columns) {
            DataType dataType = null;
            dataType = getDataType(columnInfo);
            com.ververica.cdc.common.schema.Column column =
                    com.ververica.cdc.common.schema.Column.metadataColumn(
                            columnInfo.getColumnName().toLowerCase(Locale.ROOT), dataType);
            list.add(column);
        }
        return Schema.newBuilder().setColumns(list).primaryKey(pks).build();
    }

    private DataType getDataType(ColumnInfo columnInfo) {
        String type = columnInfo.getDataType();
        DataType dataType;
        switch (type) {
            case "VARCHAR2":
            case "CHAR":
                dataType =
                        columnInfo.getDataLength() == 0
                                ? new VarCharType(255)
                                : new VarCharType(columnInfo.getDataLength());
                break;
            case "BLOB":
            case "CLOB":
            case "TEXT":
                dataType = DataTypes.STRING();
                break;
            case "NUMBER":
                dataType = new IntType();
                break;
            case "LONG":
                dataType = new BigIntType();
                break;
            case "DATE":
                dataType = new TimestampType();
                break;
            case "FLOAT":
                dataType = new FloatType();
                break;
            case "TIMESTAMP(1)":
            case "TIMESTAMP(3)":
            case "TIMESTAMP(6)":
            case "TIMESTAMP(9)":
                dataType = new TimestampType();
                break;
            case "TIMESTAMP(9) WITH TIME ZONE":
            case "TIMESTAMP(6) WITH TIME ZONE":
            case "TIMESTAMP(3) WITH TIME ZONE":
            case "TIMESTAMP(13) WITH TIME ZONE":
                dataType = new ZonedTimestampType();
                break;
            case "TIMESTAMP(6) WITH LOCAL TIME ZONE":
                dataType = new LocalZonedTimestampType();
                break;
            default:
                throw new RuntimeException("Unsupported data type:" + type);
        }
        return dataType;
    }

    private List<ColumnInfo> showCreateTable(JdbcConnection jdbc, TableId tableId) {
        List<ColumnInfo> list = new ArrayList<>();
        final String showCreateTableQuery =
                String.format(
                        "select COLUMN_NAME,DATA_TYPE,DATA_LENGTH from all_tab_columns where Table_Name='%s' order by COLUMN_ID",
                        tableId.table());
        try {
            return jdbc.queryAndMap(
                    showCreateTableQuery,
                    rs -> {
                        while (rs.next()) {
                            ColumnInfo columnInfo = new ColumnInfo();
                            String columnName = null;
                            String type = null;
                            Integer dataLength = null;
                            columnName = rs.getString(1);
                            type = rs.getString(2);
                            dataLength = rs.getInt(3);
                            columnInfo.setColumnName(columnName);
                            columnInfo.setDataType(type);
                            columnInfo.setDataLength(dataLength);
                            list.add(columnInfo);
                        }
                        return list;
                    });
        } catch (SQLException e) {
            throw new RuntimeException(
                    String.format("Failed to show create table for %s", tableId), e);
        }
    }

    private List<String> getTablePks(JdbcConnection jdbc, TableId tableId) {
        List<String> list = new ArrayList<>();
        final String showCreateTableQuery =
                String.format(
                        "SELECT COLUMN_NAME FROM all_constraints cons, all_cons_columns cols WHERE cols.table_name = '%s' and cols.OWNER='%s' AND cons.constraint_type = 'P' AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner ORDER BY cols.table_name, cols.position ",
                        tableId.table().toUpperCase(), tableId.catalog().toUpperCase());
        try {
            return jdbc.queryAndMap(
                    showCreateTableQuery,
                    rs -> {
                        while (rs.next()) {
                            String columnName = null;
                            list.add(columnName.toLowerCase(Locale.ROOT));
                        }
                        return list;
                    });
        } catch (SQLException e) {
            throw new RuntimeException(String.format("Failed to get table pks for %s", tableId), e);
        }
    }
}
