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

package com.ververica.cdc.connectors.pgsql.utils;

import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.schema.Selectors;
import com.ververica.cdc.common.types.BigIntType;
import com.ververica.cdc.common.types.BooleanType;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.common.types.DateType;
import com.ververica.cdc.common.types.DecimalType;
import com.ververica.cdc.common.types.DoubleType;
import com.ververica.cdc.common.types.FloatType;
import com.ververica.cdc.common.types.IntType;
import com.ververica.cdc.common.types.SmallIntType;
import com.ververica.cdc.common.types.TimestampType;
import com.ververica.cdc.common.types.VarCharType;
import com.ververica.cdc.common.types.ZonedTimestampType;
import com.ververica.cdc.connectors.pgsql.dto.ColumnInfo;
import com.ververica.cdc.connectors.postgres.source.PostgresDialect;
import com.ververica.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/** Utilities for converting from debezium {@link Table} types to {@link Schema}. */
public class PostgresSchemaUtils {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresSchemaUtils.class);

    // ------ MySQL Type ------
    // https://dev.mysql.com/doc/refman/8.0/en/data-types.html
    private static final String BIT = "BIT";
    /*
     * BOOLEAN type will be returned when handling the change event from SQL like:
     * ALTER TABLE `student` CHANGE COLUMN `is_male` `is_female` BOOLEAN NULL;
     */
    private static final String BOOLEAN = "BOOLEAN";
    private static final String BOOL = "BOOL";
    private static final String TINYINT = "TINYINT";
    private static final String TINYINT_UNSIGNED = "TINYINT UNSIGNED";
    private static final String TINYINT_UNSIGNED_ZEROFILL = "TINYINT UNSIGNED ZEROFILL";
    private static final String SMALLINT = "SMALLINT";
    private static final String SMALLINT_UNSIGNED = "SMALLINT UNSIGNED";
    private static final String SMALLINT_UNSIGNED_ZEROFILL = "SMALLINT UNSIGNED ZEROFILL";
    private static final String MEDIUMINT = "MEDIUMINT";
    private static final String MEDIUMINT_UNSIGNED = "MEDIUMINT UNSIGNED";
    private static final String MEDIUMINT_UNSIGNED_ZEROFILL = "MEDIUMINT UNSIGNED ZEROFILL";
    private static final String INT = "INT";
    private static final String INT_UNSIGNED = "INT UNSIGNED";
    private static final String INT_UNSIGNED_ZEROFILL = "INT UNSIGNED ZEROFILL";
    private static final String INTEGER = "INTEGER";
    private static final String INTEGER_UNSIGNED = "INTEGER UNSIGNED";
    private static final String INTEGER_UNSIGNED_ZEROFILL = "INTEGER UNSIGNED ZEROFILL";
    private static final String BIGINT = "BIGINT";
    private static final String SERIAL = "SERIAL";
    private static final String BIGINT_UNSIGNED = "BIGINT UNSIGNED";
    private static final String BIGINT_UNSIGNED_ZEROFILL = "BIGINT UNSIGNED ZEROFILL";
    private static final String REAL = "REAL";
    private static final String REAL_UNSIGNED = "REAL UNSIGNED";
    private static final String REAL_UNSIGNED_ZEROFILL = "REAL UNSIGNED ZEROFILL";
    private static final String FLOAT = "FLOAT";
    private static final String FLOAT_UNSIGNED = "FLOAT UNSIGNED";
    private static final String FLOAT_UNSIGNED_ZEROFILL = "FLOAT UNSIGNED ZEROFILL";
    private static final String DOUBLE = "DOUBLE";
    private static final String DOUBLE_UNSIGNED = "DOUBLE UNSIGNED";
    private static final String DOUBLE_UNSIGNED_ZEROFILL = "DOUBLE UNSIGNED ZEROFILL";
    private static final String DOUBLE_PRECISION = "DOUBLE PRECISION";
    private static final String DOUBLE_PRECISION_UNSIGNED = "DOUBLE PRECISION UNSIGNED";
    private static final String DOUBLE_PRECISION_UNSIGNED_ZEROFILL =
            "DOUBLE PRECISION UNSIGNED ZEROFILL";
    private static final String NUMERIC = "NUMERIC";
    private static final String NUMERIC_UNSIGNED = "NUMERIC UNSIGNED";
    private static final String NUMERIC_UNSIGNED_ZEROFILL = "NUMERIC UNSIGNED ZEROFILL";
    private static final String FIXED = "FIXED";
    private static final String FIXED_UNSIGNED = "FIXED UNSIGNED";
    private static final String FIXED_UNSIGNED_ZEROFILL = "FIXED UNSIGNED ZEROFILL";
    private static final String DECIMAL = "DECIMAL";
    private static final String DECIMAL_UNSIGNED = "DECIMAL UNSIGNED";
    private static final String DECIMAL_UNSIGNED_ZEROFILL = "DECIMAL UNSIGNED ZEROFILL";
    private static final String CHAR = "CHAR";
    private static final String VARCHAR = "VARCHAR";
    private static final String TINYTEXT = "TINYTEXT";
    private static final String MEDIUMTEXT = "MEDIUMTEXT";
    private static final String TEXT = "TEXT";
    private static final String LONGTEXT = "LONGTEXT";
    private static final String DATE = "DATE";
    private static final String TIME = "TIME";
    private static final String DATETIME = "DATETIME";
    private static final String TIMESTAMP = "TIMESTAMP";
    private static final String YEAR = "YEAR";
    private static final String BINARY = "BINARY";
    private static final String VARBINARY = "VARBINARY";
    private static final String TINYBLOB = "TINYBLOB";
    private static final String MEDIUMBLOB = "MEDIUMBLOB";
    private static final String BLOB = "BLOB";
    private static final String LONGBLOB = "LONGBLOB";
    private static final String JSON = "JSON";
    private static final String SET = "SET";
    private static final String ENUM = "ENUM";
    private static final String GEOMETRY = "GEOMETRY";
    private static final String POINT = "POINT";
    private static final String LINESTRING = "LINESTRING";
    private static final String POLYGON = "POLYGON";
    private static final String GEOMCOLLECTION = "GEOMCOLLECTION";
    private static final String GEOMETRYCOLLECTION = "GEOMETRYCOLLECTION";
    private static final String MULTIPOINT = "MULTIPOINT";
    private static final String MULTIPOLYGON = "MULTIPOLYGON";
    private static final String MULTILINESTRING = "MULTILINESTRING";
    private static final String UNKNOWN = "UNKNOWN";

    /** Creates a new {@link PostgresConnection}, but not open the connection. */
    public static PostgresConnection createPostgresConnection(PostgresSourceConfig sourceConfig) {
        PostgresDialect dialect = new PostgresDialect(sourceConfig);
        return dialect.openJdbcConnection();
    }

    public static List<TableId> listTables(
            PostgresSourceConfig sourceConfig, @Nullable String dbName) {
        try (PostgresConnection jdbc = createPostgresConnection(sourceConfig)) {
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

    public static Schema getTableSchema(
            PostgresSourceConfig sourceConfig, PostgresPartition partition, TableId tableId) {
        try (PostgresConnection jdbc = createPostgresConnection(sourceConfig)) {
            return getTableSchema(partition, tableId, sourceConfig, jdbc);
        } catch (Exception e) {
            throw new RuntimeException("Error to get table schema: " + e.getMessage(), e);
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
                "SELECT datname FROM pg_database WHERE datistemplate = false ;",
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
                "SELECT table_catalog,table_schema,table_name FROM information_schema.tables WHERE table_catalog = '"
                        + dbName
                        + "' ;",
                rs -> {
                    while (rs.next()) {
                        tableIds.add(
                                TableId.tableId(rs.getString(1), rs.getString(2), rs.getString(3)));
                    }
                });
        LOG.info("\t list of available tables are: {}", tableIds);
        return tableIds;
    }

    public static Schema getTableSchema(
            PostgresPartition partition,
            TableId tableId,
            PostgresSourceConfig sourceConfig,
            PostgresConnection jdbc) {
        // fetch table schemas
        return null;
    }

    public static Schema toSchema(Table table) {
        List<Column> columns =
                table.columns().stream()
                        .map(PostgresSchemaUtils::toColumn)
                        .collect(Collectors.toList());

        return Schema.newBuilder()
                .setColumns(columns)
                .primaryKey(table.primaryKeyColumnNames())
                .comment(table.comment())
                .build();
    }

    public static Column toColumn(io.debezium.relational.Column column) {
        return Column.physicalColumn(column.name(), fromDbzColumn(column), column.comment());
    }

    public static io.debezium.relational.TableId toDbzTableId(TableId tableId) {
        return new io.debezium.relational.TableId(
                tableId.getSchemaName(), null, tableId.getTableName());
    }

    /**
     * Returns a corresponding Flink data type from a debezium {@link
     * io.debezium.relational.Column}.
     */
    public static DataType fromDbzColumn(io.debezium.relational.Column column) {
        DataType dataType = convertFromColumn(column);
        if (column.isOptional()) {
            return dataType;
        } else {
            return dataType.notNull();
        }
    }

    /**
     * Returns a corresponding Flink data type from a debezium {@link io.debezium.relational.Column}
     * with nullable always be true.
     */
    private static DataType convertFromColumn(io.debezium.relational.Column column) {
        String typeName = column.typeName();
        switch (typeName) {
            case BIT:
                return column.length() == 1
                        ? DataTypes.BOOLEAN()
                        : DataTypes.BINARY((column.length() + 7) / 8);
            case BOOL:
            case BOOLEAN:
                return DataTypes.BOOLEAN();
            case TINYINT:
                // MySQL haven't boolean type, it uses tinyint(1) to represents boolean type
                // user should not use tinyint(1) to store number although jdbc url parameter
                // tinyInt1isBit=false can help change the return value, it's not a general way
                // btw: mybatis and mysql-connector-java map tinyint(1) to boolean by default
                return column.length() == 1 ? DataTypes.BOOLEAN() : DataTypes.TINYINT();
            case TINYINT_UNSIGNED:
            case TINYINT_UNSIGNED_ZEROFILL:
            case SMALLINT:
                return DataTypes.SMALLINT();
            case SMALLINT_UNSIGNED:
            case SMALLINT_UNSIGNED_ZEROFILL:
            case INT:
            case INTEGER:
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
            case MEDIUMINT_UNSIGNED_ZEROFILL:
            case YEAR:
                return DataTypes.INT();
            case INT_UNSIGNED:
            case INT_UNSIGNED_ZEROFILL:
            case INTEGER_UNSIGNED:
            case INTEGER_UNSIGNED_ZEROFILL:
            case BIGINT:
                return DataTypes.BIGINT();
            case BIGINT_UNSIGNED:
            case BIGINT_UNSIGNED_ZEROFILL:
            case SERIAL:
                return DataTypes.DECIMAL(20, 0);
            case FLOAT:
            case FLOAT_UNSIGNED:
            case FLOAT_UNSIGNED_ZEROFILL:
                return DataTypes.FLOAT();
            case REAL:
            case REAL_UNSIGNED:
            case REAL_UNSIGNED_ZEROFILL:
            case DOUBLE:
            case DOUBLE_UNSIGNED:
            case DOUBLE_UNSIGNED_ZEROFILL:
            case DOUBLE_PRECISION:
            case DOUBLE_PRECISION_UNSIGNED:
            case DOUBLE_PRECISION_UNSIGNED_ZEROFILL:
                return DataTypes.DOUBLE();
            case NUMERIC:
            case NUMERIC_UNSIGNED:
            case NUMERIC_UNSIGNED_ZEROFILL:
            case FIXED:
            case FIXED_UNSIGNED:
            case FIXED_UNSIGNED_ZEROFILL:
            case DECIMAL:
            case DECIMAL_UNSIGNED:
            case DECIMAL_UNSIGNED_ZEROFILL:
                return column.length() <= 38
                        ? DataTypes.DECIMAL(column.length(), column.scale().orElse(0))
                        : DataTypes.STRING();
            case TIME:
                return column.length() >= 0 ? DataTypes.TIME(column.length()) : DataTypes.TIME();
            case DATE:
                return DataTypes.DATE();
            case DATETIME:
                return column.length() >= 0
                        ? DataTypes.TIMESTAMP(column.length())
                        : DataTypes.TIMESTAMP(0);
            case TIMESTAMP:
                return column.length() >= 0
                        ? DataTypes.TIMESTAMP_LTZ(column.length())
                        : DataTypes.TIMESTAMP_LTZ(0);
            case CHAR:
                return DataTypes.CHAR(column.length());
            case VARCHAR:
                return DataTypes.VARCHAR(column.length());
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
            case JSON:
            case ENUM:
            case GEOMETRY:
            case POINT:
            case LINESTRING:
            case POLYGON:
            case GEOMETRYCOLLECTION:
            case GEOMCOLLECTION:
            case MULTIPOINT:
            case MULTIPOLYGON:
            case MULTILINESTRING:
                return DataTypes.STRING();
            case BINARY:
                return DataTypes.BINARY(column.length());
            case VARBINARY:
                return DataTypes.VARBINARY(column.length());
            case TINYBLOB:
            case BLOB:
            case MEDIUMBLOB:
            case LONGBLOB:
                return DataTypes.BYTES();
            case SET:
                return DataTypes.ARRAY(DataTypes.STRING());
            default:
                throw new UnsupportedOperationException(
                        String.format("Don't support MySQL type '%s' yet.", typeName));
        }
    }

    public static Schema getSchema(JdbcConnection jdbc, io.debezium.relational.TableId tableId) {
        List<ColumnInfo> columns = showCreateTable(jdbc, tableId);
        List<String> pks = getTablePks(jdbc, tableId);
        List<com.ververica.cdc.common.schema.Column> list = new ArrayList<>();
        for (ColumnInfo columnInfo : columns) {
            DataType dataType = null;
            dataType = getDataType(columnInfo);
            com.ververica.cdc.common.schema.Column column =
                    com.ververica.cdc.common.schema.Column.metadataColumn(
                            columnInfo.getColumnName(), dataType);
            list.add(column);
        }
        return Schema.newBuilder().setColumns(list).primaryKey(pks).build();
    }

    private static DataType getDataType(ColumnInfo columnInfo) {
        DataType dataType;
        switch (columnInfo.getUdtName()) {
            case "varchar":
            case "char":
            case "bpchar":
                dataType =
                        columnInfo.getCharacterMaximumLength() == 0
                                ? new VarCharType(255)
                                : new VarCharType(columnInfo.getCharacterMaximumLength());
                break;
            case "bytea":
            case "json":
            case "text":
            case "point":
            case "line":
            case "box":
            case "polygon":
                dataType = DataTypes.STRING();
                break;
            case "timestamptz":
                dataType =
                        columnInfo.getDatetimePrecision() >= 0
                                ? new ZonedTimestampType(columnInfo.getDatetimePrecision())
                                : new ZonedTimestampType();
                break;
            case "int8":
                dataType = new BigIntType();
                break;
            case "float4":
                dataType = new FloatType();
                break;
            case "float8":
                dataType = new DoubleType();
                break;
            case "timestamp":
                dataType =
                        columnInfo.getDatetimePrecision() >= 0
                                ? new TimestampType(columnInfo.getDatetimePrecision())
                                : new TimestampType();
                break;
            case "numeric":
                dataType =
                        columnInfo.getNumericPrecision() == 0
                                        || columnInfo.getNumericPrecision() == null
                                ? new DecimalType()
                                : new DecimalType(
                                        columnInfo.getNumericPrecision(),
                                        columnInfo.getNumericScale());
                break;
            case "date":
                dataType = new DateType();
                break;
            case "bool":
                dataType = new BooleanType();
                break;
            case "time":
                dataType =
                        columnInfo.getDatetimePrecision() >= 0
                                ? DataTypes.TIME(columnInfo.getDatetimePrecision())
                                : DataTypes.TIME();
                break;
            case "int4":
                dataType = new IntType();
                break;
            case "int2":
                dataType = new SmallIntType();
                break;
            default:
                throw new RuntimeException(
                        "Unsupported data type:"
                                + columnInfo.getColumnName()
                                + " "
                                + columnInfo.getUdtName());
        }
        if (columnInfo.getIsNullable().equals("YES")) {
            dataType = dataType.nullable();
        } else {
            dataType = dataType.notNull();
        }
        return dataType;
    }

    private static List<ColumnInfo> showCreateTable(
            JdbcConnection jdbc, io.debezium.relational.TableId tableId) {
        //        List<Map<String, String>> list = new ArrayList<>();
        List<ColumnInfo> columnInfos = new ArrayList<>();
        final String showCreateTableQuery =
                String.format(
                        "SELECT column_name,udt_name,character_maximum_length,numeric_precision,numeric_scale,is_nullable,column_default,datetime_precision FROM information_schema.columns WHERE table_name ='%s' and table_catalog='%s' and table_schema='%s' order by ordinal_position",
                        tableId.table(), tableId.catalog(), tableId.schema());
        try {

            return jdbc.queryAndMap(
                    showCreateTableQuery,
                    rs -> {
                        while (rs.next()) {
                            ColumnInfo columnInfo = new ColumnInfo();
                            columnInfo.setColumnName(rs.getString(1));
                            columnInfo.setUdtName(rs.getString(2));
                            columnInfo.setCharacterMaximumLength(rs.getInt(3));
                            columnInfo.setNumericPrecision(rs.getInt(4));
                            columnInfo.setNumericScale(rs.getInt(5));
                            columnInfo.setIsNullable(rs.getString(6));
                            columnInfo.setColumnDefault(rs.getString(7));
                            columnInfo.setDatetimePrecision(rs.getInt(8));
                            columnInfos.add(columnInfo);
                        }
                        return columnInfos;
                    });
        } catch (SQLException e) {
            throw new RuntimeException(
                    String.format("Failed to show create table for %s", tableId), e);
        }
    }

    private static List<String> getTablePks(
            JdbcConnection jdbc, io.debezium.relational.TableId tableId) {
        List<String> list = new ArrayList<>();
        final String showCreateTableQuery =
                String.format(
                        "SELECT kcu.column_name FROM information_schema.table_constraints AS tc JOIN information_schema.key_column_usage  AS kcu ON tc.constraint_name = kcu.constraint_name WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_catalog='%s' AND tc.table_schema='%s' AND kcu.table_schema ='%s' AND tc.table_name = '%s' order by ordinal_position",
                        tableId.catalog(), tableId.schema(), tableId.schema(), tableId.table());
        try {
            return jdbc.queryAndMap(
                    showCreateTableQuery,
                    rs -> {
                        while (rs.next()) {
                            String columnName = null;
                            columnName = rs.getString(1);
                            list.add(columnName);
                        }
                        return list;
                    });
        } catch (SQLException e) {
            throw new RuntimeException(String.format("Failed to get table pks for %s", tableId), e);
        }
    }

    public static List<io.debezium.relational.TableId> getCapturedTableIds(
            PostgresSourceConfig sourceConfig) {
        List<String> tableList = sourceConfig.getTableList();
        String database = sourceConfig.getDatabaseList().get(0);
        tableList = tableList.stream().map(e -> database + "." + e).collect(Collectors.toList());
        Selectors selectors =
                new Selectors.SelectorsBuilder()
                        .includeTables(String.join(", ", tableList))
                        .build();
        String[] capturedTables = getTableList(sourceConfig, selectors);
        List<io.debezium.relational.TableId> capturedTableIds = new ArrayList<>();
        for (String table : capturedTables) {
            io.debezium.relational.TableId capturedTableId =
                    io.debezium.relational.TableId.parse(table);
            capturedTableIds.add(capturedTableId);
        }
        return capturedTableIds;
    }

    private static String[] getTableList(PostgresSourceConfig sourceConfig, Selectors selectors) {
        return listTables(sourceConfig, null).stream()
                .filter(selectors::isMatch)
                .map(com.ververica.cdc.common.event.TableId::toString)
                .toArray(String[]::new);
    }

    private PostgresSchemaUtils() {}
}
