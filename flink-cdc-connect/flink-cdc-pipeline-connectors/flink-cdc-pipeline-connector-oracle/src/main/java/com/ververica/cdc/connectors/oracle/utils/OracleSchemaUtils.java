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

package com.ververica.cdc.connectors.oracle.utils;

import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.types.BigIntType;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.common.types.DecimalType;
import com.ververica.cdc.common.types.FloatType;
import com.ververica.cdc.common.types.LocalZonedTimestampType;
import com.ververica.cdc.common.types.TimestampType;
import com.ververica.cdc.common.types.VarCharType;
import com.ververica.cdc.common.types.ZonedTimestampType;
import com.ververica.cdc.connectors.oracle.dto.ColumnInfo;
import com.ververica.cdc.connectors.oracle.source.config.OracleSourceConfig;
import com.ververica.cdc.connectors.oracle.source.utils.OracleSchema;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Table;
import io.debezium.relational.history.TableChanges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static com.ververica.cdc.connectors.oracle.source.utils.OracleUtils.quote;

/** Utilities for converting from debezium {@link Table} types to {@link Schema}. */
public class OracleSchemaUtils {

    private static final Logger LOG = LoggerFactory.getLogger(OracleSchemaUtils.class);

    public static List<TableId> listTables(
            OracleSourceConfig sourceConfig, @Nullable String dbName) {
        try (JdbcConnection jdbc = DebeziumUtils.createOracleConnection(sourceConfig)) {
            List<String> databases =
                    dbName != null ? Collections.singletonList(dbName) : listDatabases(jdbc);

            List<TableId> tableIds = new ArrayList<>();
            for (String database : databases) {
                tableIds.addAll(listTables(jdbc, database));
            }
            return tableIds;
        } catch (SQLException e) {
            throw new RuntimeException("Error to list databases: " + e.getMessage(), e);
        }
    }

    public static List<String> listDatabases(OracleSourceConfig sourceConfig) {
        try (JdbcConnection jdbc = DebeziumUtils.createOracleConnection(sourceConfig)) {
            return listDatabases(jdbc);
        } catch (SQLException e) {
            throw new RuntimeException("Error to list databases: " + e.getMessage(), e);
        }
    }

    public static List<String> listDatabases(JdbcConnection jdbc) throws SQLException {
        // -------------------
        // READ DATABASE NAMES
        // -------------------
        // Get the list of databases ...
        LOG.info("Read list of available databases");
        final List<String> databaseNames = new ArrayList<>();
        jdbc.query(
                "SHOW DATABASES WHERE `database` NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')",
                rs -> {
                    while (rs.next()) {
                        databaseNames.add(rs.getString(1));
                    }
                });
        LOG.info("\t list of available databases are: {}", databaseNames);
        return databaseNames;
    }

    public static List<TableId> listTables(JdbcConnection jdbc, String dbName) throws SQLException {
        // ----------------
        // READ TABLE NAMES
        // ----------------
        // Get the list of table IDs for each database. We can't use a prepared statement with
        // MySQL, so we have to build the SQL statement each time. Although in other cases this
        // might lead to SQL injection, in our case we are reading the database names from the
        // database and not taking them from the user ...
        LOG.info("Read list of available tables in {}", dbName);
        final List<TableId> tableIds = new ArrayList<>();
        jdbc.query(
                "SHOW FULL TABLES IN " + quote(dbName) + " where Table_Type = 'BASE TABLE'",
                rs -> {
                    while (rs.next()) {
                        tableIds.add(TableId.tableId(dbName, rs.getString(1)));
                    }
                });
        LOG.info("\t list of available tables are: {}", tableIds);
        return tableIds;
    }

    public static Schema getTableSchema(TableId tableId, OracleSourceConfig sourceConfig) {
        try {
            // fetch table schemas
            JdbcConnection jdbc = DebeziumUtils.createOracleConnection(sourceConfig);
            OracleSchema mySqlSchema = new OracleSchema();
            TableChanges.TableChange tableSchema =
                    mySqlSchema.getTableSchema(jdbc, toDbzTableId(tableId));
            return toSchema(tableSchema.getTable());
        } catch (Exception e) {
            throw new RuntimeException("Error to get table schema: " + e.getMessage(), e);
        }
    }

    public static Schema toSchema(Table table) {
        List<Column> columns =
                table.columns().stream()
                        .map(OracleSchemaUtils::toColumn)
                        .collect(Collectors.toList());

        return Schema.newBuilder()
                .setColumns(columns)
                .primaryKey(table.primaryKeyColumnNames())
                .comment(table.comment())
                .build();
    }

    public static Column toColumn(io.debezium.relational.Column column) {
        return Column.physicalColumn(
                column.name(), OracleTypeUtils.fromDbzColumn(column), column.comment());
    }

    public static io.debezium.relational.TableId toDbzTableId(TableId tableId) {
        return new io.debezium.relational.TableId(
                tableId.getSchemaName(), null, tableId.getTableName());
    }

    private OracleSchemaUtils() {}

    public static Schema getSchema(JdbcConnection jdbc, io.debezium.relational.TableId tableId) {
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

    public static DataType getDataType(ColumnInfo columnInfo) {
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
                dataType =
                        columnInfo.getDataPrecision() == null || columnInfo.getDataPrecision() == 0
                                ? new DecimalType()
                                : new DecimalType(
                                        columnInfo.getDataPrecision(), columnInfo.getDataScale());
                break;
            case "LONG":
                dataType = new BigIntType();
                break;
            case "DATE":
                dataType = new TimestampType(6);
                break;
            case "FLOAT":
                dataType = new FloatType();
                break;
            case "TIMESTAMP(1)":
            case "TIMESTAMP(3)":
            case "TIMESTAMP(6)":
            case "TIMESTAMP(9)":
                dataType = new TimestampType(6);
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

    public static List<ColumnInfo> showCreateTable(
            JdbcConnection jdbc, io.debezium.relational.TableId tableId) {
        List<ColumnInfo> list = new ArrayList<>();
        final String showCreateTableQuery =
                String.format(
                        "select COLUMN_NAME,DATA_TYPE,DATA_LENGTH,DATA_PRECISION,DATA_SCALE from all_tab_columns where Table_Name='%s' and OWNER='%s' order by COLUMN_ID",
                        tableId.table(), tableId.catalog());
        try {
            return jdbc.queryAndMap(
                    showCreateTableQuery,
                    rs -> {
                        while (rs.next()) {
                            ColumnInfo columnInfo = new ColumnInfo();
                            columnInfo.setColumnName(rs.getString(1));
                            columnInfo.setDataType(rs.getString(2));
                            columnInfo.setDataLength(rs.getInt(3));
                            columnInfo.setDataPrecision(rs.getInt(4));
                            columnInfo.setDataScale(rs.getInt(5));
                            list.add(columnInfo);
                        }
                        return list;
                    });
        } catch (SQLException e) {
            throw new RuntimeException(
                    String.format("Failed to show create table for %s", tableId), e);
        }
    }

    public static List<String> getTablePks(
            JdbcConnection jdbc, io.debezium.relational.TableId tableId) {
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
                            columnName = rs.getString(1);
                            list.add(columnName.toLowerCase(Locale.ROOT));
                        }
                        return list;
                    });
        } catch (SQLException e) {
            throw new RuntimeException(String.format("Failed to get table pks for %s", tableId), e);
        }
    }
}
