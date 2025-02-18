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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;

import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.schema.Selectors;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.connectors.oracle.dto.JdbcInfo;
import com.ververica.cdc.connectors.oracle.dto.MySqlFieldDefinition;
import com.ververica.cdc.connectors.oracle.dto.MySqlTableDefinition;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.Tables;
import io.debezium.text.ParsingException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Utilities related to Debezium. */
public class SchemaUtils {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaUtils.class);

    private static MySqlAntlrDdlParser mySqlAntlrDdlParser;

    static final String BIT = "BIT";
    /*
     * BOOLEAN type will be returned when handling the change event from SQL like:
     * ALTER TABLE `student` CHANGE COLUMN `is_male` `is_female` BOOLEAN NULL;
     */
    static final String BOOLEAN = "BOOLEAN";
    static final String BOOL = "BOOL";
    static final String TINYINT = "TINYINT";
    static final String TINYINT_UNSIGNED = "TINYINT UNSIGNED";
    static final String TINYINT_UNSIGNED_ZEROFILL = "TINYINT UNSIGNED ZEROFILL";
    static final String SMALLINT = "SMALLINT";
    static final String SMALLINT_UNSIGNED = "SMALLINT UNSIGNED";
    static final String SMALLINT_UNSIGNED_ZEROFILL = "SMALLINT UNSIGNED ZEROFILL";
    static final String MEDIUMINT = "MEDIUMINT";
    static final String MEDIUMINT_UNSIGNED = "MEDIUMINT UNSIGNED";
    static final String MEDIUMINT_UNSIGNED_ZEROFILL = "MEDIUMINT UNSIGNED ZEROFILL";
    static final String INT = "INT";
    static final String INT_UNSIGNED = "INT UNSIGNED";
    static final String INT_UNSIGNED_ZEROFILL = "INT UNSIGNED ZEROFILL";
    static final String INTEGER = "INTEGER";
    static final String INTEGER_UNSIGNED = "INTEGER UNSIGNED";
    static final String INTEGER_UNSIGNED_ZEROFILL = "INTEGER UNSIGNED ZEROFILL";
    static final String BIGINT = "BIGINT";
    static final String SERIAL = "SERIAL";
    static final String BIGINT_UNSIGNED = "BIGINT UNSIGNED";
    static final String BIGINT_UNSIGNED_ZEROFILL = "BIGINT UNSIGNED ZEROFILL";
    static final String REAL = "REAL";
    static final String REAL_UNSIGNED = "REAL UNSIGNED";
    static final String REAL_UNSIGNED_ZEROFILL = "REAL UNSIGNED ZEROFILL";
    static final String FLOAT = "FLOAT";
    static final String FLOAT_UNSIGNED = "FLOAT UNSIGNED";
    static final String FLOAT_UNSIGNED_ZEROFILL = "FLOAT UNSIGNED ZEROFILL";
    static final String DOUBLE = "DOUBLE";
    static final String DOUBLE_UNSIGNED = "DOUBLE UNSIGNED";
    static final String DOUBLE_UNSIGNED_ZEROFILL = "DOUBLE UNSIGNED ZEROFILL";
    static final String DOUBLE_PRECISION = "DOUBLE PRECISION";
    static final String DOUBLE_PRECISION_UNSIGNED = "DOUBLE PRECISION UNSIGNED";
    static final String DOUBLE_PRECISION_UNSIGNED_ZEROFILL = "DOUBLE PRECISION UNSIGNED ZEROFILL";
    static final String NUMERIC = "NUMERIC";
    static final String NUMERIC_UNSIGNED = "NUMERIC UNSIGNED";
    static final String NUMERIC_UNSIGNED_ZEROFILL = "NUMERIC UNSIGNED ZEROFILL";
    static final String FIXED = "FIXED";
    static final String FIXED_UNSIGNED = "FIXED UNSIGNED";
    static final String FIXED_UNSIGNED_ZEROFILL = "FIXED UNSIGNED ZEROFILL";
    static final String DECIMAL = "DECIMAL";
    static final String DECIMAL_UNSIGNED = "DECIMAL UNSIGNED";
    static final String DECIMAL_UNSIGNED_ZEROFILL = "DECIMAL UNSIGNED ZEROFILL";
    static final String CHAR = "CHAR";
    static final String VARCHAR = "VARCHAR";
    static final String TINYTEXT = "TINYTEXT";
    static final String MEDIUMTEXT = "MEDIUMTEXT";
    static final String TEXT = "TEXT";
    static final String LONGTEXT = "LONGTEXT";
    static final String DATE = "DATE";
    static final String TIME = "TIME";
    static final String DATETIME = "DATETIME";
    static final String TIMESTAMP = "TIMESTAMP";
    static final String YEAR = "YEAR";
    static final String BINARY = "BINARY";
    static final String VARBINARY = "VARBINARY";
    static final String TINYBLOB = "TINYBLOB";
    static final String MEDIUMBLOB = "MEDIUMBLOB";
    static final String BLOB = "BLOB";
    static final String LONGBLOB = "LONGBLOB";
    static final String JSON = "JSON";
    static final String SET = "SET";
    static final String ENUM = "ENUM";
    static final String GEOMETRY = "GEOMETRY";
    static final String POINT = "POINT";
    static final String LINESTRING = "LINESTRING";
    static final String POLYGON = "POLYGON";
    static final String GEOMCOLLECTION = "GEOMCOLLECTION";
    static final String GEOMETRYCOLLECTION = "GEOMETRYCOLLECTION";
    static final String MULTIPOINT = "MULTIPOINT";
    static final String MULTIPOLYGON = "MULTIPOLYGON";
    static final String MULTILINESTRING = "MULTILINESTRING";
    static final String UNKNOWN = "UNKNOWN";
    static final int FLOAT_LENGTH_UNSPECIFIED_FLAG = -1;

    /**
     * Returns a corresponding Flink data type from a debezium {@link Column} with nullable always
     * be true.
     */
    static DataType convertFromColumn(Column column) {
        String typeName = column.typeName();
        switch (typeName) {
            case BIT:
                return column.length() == 1
                        ? com.ververica.cdc.common.types.DataTypes.BOOLEAN()
                        : com.ververica.cdc.common.types.DataTypes.BINARY(
                                (column.length() + 7) / 8);
            case BOOL:
            case BOOLEAN:
                return com.ververica.cdc.common.types.DataTypes.BOOLEAN();
            case TINYINT:
                return column.length() == 1
                        ? com.ververica.cdc.common.types.DataTypes.BOOLEAN()
                        : com.ververica.cdc.common.types.DataTypes.TINYINT();
            case TINYINT_UNSIGNED:
            case TINYINT_UNSIGNED_ZEROFILL:
            case SMALLINT:
                return com.ververica.cdc.common.types.DataTypes.SMALLINT();
            case SMALLINT_UNSIGNED:
            case SMALLINT_UNSIGNED_ZEROFILL:
            case INT:
            case INTEGER:
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
            case MEDIUMINT_UNSIGNED_ZEROFILL:
            case YEAR:
                return com.ververica.cdc.common.types.DataTypes.INT();
            case INT_UNSIGNED:
            case INT_UNSIGNED_ZEROFILL:
            case INTEGER_UNSIGNED:
            case INTEGER_UNSIGNED_ZEROFILL:
            case BIGINT:
                return com.ververica.cdc.common.types.DataTypes.BIGINT();
            case BIGINT_UNSIGNED:
            case BIGINT_UNSIGNED_ZEROFILL:
            case SERIAL:
                return com.ververica.cdc.common.types.DataTypes.DECIMAL(20, 0);
            case FLOAT:
            case FLOAT_UNSIGNED:
            case FLOAT_UNSIGNED_ZEROFILL:
                return com.ververica.cdc.common.types.DataTypes.FLOAT();
            case REAL:
            case REAL_UNSIGNED:
            case REAL_UNSIGNED_ZEROFILL:
            case DOUBLE:
            case DOUBLE_UNSIGNED:
            case DOUBLE_UNSIGNED_ZEROFILL:
            case DOUBLE_PRECISION:
            case DOUBLE_PRECISION_UNSIGNED:
            case DOUBLE_PRECISION_UNSIGNED_ZEROFILL:
                return com.ververica.cdc.common.types.DataTypes.DOUBLE();
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
                        ? com.ververica.cdc.common.types.DataTypes.DECIMAL(
                                column.length(), column.scale().orElse(0))
                        : com.ververica.cdc.common.types.DataTypes.STRING();
            case TIME:
                return column.length() >= 0
                        ? com.ververica.cdc.common.types.DataTypes.TIME(column.length())
                        : com.ververica.cdc.common.types.DataTypes.TIME();
            case DATE:
                return com.ververica.cdc.common.types.DataTypes.DATE();
            case DATETIME:
                return column.length() >= 0
                        ? com.ververica.cdc.common.types.DataTypes.TIMESTAMP(column.length())
                        : com.ververica.cdc.common.types.DataTypes.TIMESTAMP(0);
            case TIMESTAMP:
                return column.length() >= 0
                        ? com.ververica.cdc.common.types.DataTypes.TIMESTAMP_LTZ(column.length())
                        : com.ververica.cdc.common.types.DataTypes.TIMESTAMP_LTZ(0);
            case CHAR:
                return com.ververica.cdc.common.types.DataTypes.CHAR(column.length());
            case VARCHAR:
                return com.ververica.cdc.common.types.DataTypes.VARCHAR(column.length());
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
                return com.ververica.cdc.common.types.DataTypes.STRING();
            case BINARY:
                return com.ververica.cdc.common.types.DataTypes.BINARY(column.length());
            case VARBINARY:
                return com.ververica.cdc.common.types.DataTypes.VARBINARY(column.length());
            case TINYBLOB:
            case BLOB:
            case MEDIUMBLOB:
            case LONGBLOB:
                return com.ververica.cdc.common.types.DataTypes.BYTES();
            case SET:
                return com.ververica.cdc.common.types.DataTypes.ARRAY(
                        com.ververica.cdc.common.types.DataTypes.STRING());
            default:
                throw new UnsupportedOperationException(
                        String.format("Don't support oracle type '%s' yet.", typeName));
        }
    }

    /** Creates and opens a new {@link Connection} backing connection pool. */
    public static Connection openJdbcConnection(JdbcInfo jdbcInfo) {
        Connection connection = null;
        String jdbcUrl =
                "jdbc:mysql://"
                        + jdbcInfo.getHost()
                        + ":"
                        + jdbcInfo.getPort()
                        + "/"
                        + jdbcInfo.getDatabase()
                        + "?useSSL=false&useUnicode=true&characterEncoding=UTF-8";
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection =
                    DriverManager.getConnection(
                            jdbcUrl, jdbcInfo.getUsername(), jdbcInfo.getPassword());
            return connection;
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("JDBC driver not found");
        } catch (SQLException e) {
            throw new RuntimeException("connect failed: " + e.getMessage());
        }
    }

    public static Schema getSchema(String tableIdStr, JdbcInfo jdbcInfo) {
        TableId tableId = TableId.parse(tableIdStr);
        Connection connection = openJdbcConnection(jdbcInfo);
        String ddlStatement = showCreateTable(connection, tableId);
        try {
            return parseDDL(ddlStatement, tableId);
        } catch (ParsingException pe) {
            LOG.warn(
                    "Failed to parse DDL: \n{}\nWill try parsing by describing table.",
                    ddlStatement,
                    pe);
        }
        ddlStatement = describeTable(connection, tableId);
        return parseDDL(ddlStatement, tableId);
    }

    public static Schema parseDDL(String ddlStatement, TableId tableId) {
        Table table = parseDdl(ddlStatement, tableId);

        List<Column> columns = table.columns();
        Schema.Builder tableBuilder = Schema.newBuilder();
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);

            String colName = column.name();
            DataType dataType = fromDbzColumn(column);
            if (!column.isOptional()) {
                dataType = dataType.notNull();
            }
            tableBuilder.physicalColumn(colName, dataType, column.comment());
        }

        List<String> primaryKey = new ArrayList<>();
        primaryKey.addAll(table.primaryKeyColumnNames());
        if (Objects.nonNull(primaryKey) && !primaryKey.isEmpty()) {
            tableBuilder.primaryKey(primaryKey);
        }
        return tableBuilder.build();
    }

    /** Returns a corresponding Flink data type from a debezium {@link Column}. */
    public static DataType fromDbzColumn(Column column) {
        DataType dataType = convertFromColumn(column);
        if (column.isOptional()) {
            return dataType;
        } else {
            return dataType.notNull();
        }
    }

    public static String describeTable(Connection jdbc, TableId tableId) {
        List<MySqlFieldDefinition> fieldMetas = new ArrayList<>();
        List<String> primaryKeys = new ArrayList<>();
        try {
            ResultSet rs =
                    jdbc.createStatement()
                            .executeQuery(
                                    String.format(
                                            "DESC `%s`.`%s`",
                                            tableId.getSchemaName(), tableId.getTableName()));

            while (rs.next()) {
                MySqlFieldDefinition meta = new MySqlFieldDefinition();
                meta.setColumnName(rs.getString("Field"));
                meta.setColumnType(rs.getString("Type"));
                meta.setNullable(StringUtils.equalsIgnoreCase(rs.getString("Null"), "YES"));
                meta.setKey("PRI".equalsIgnoreCase(rs.getString("Key")));
                meta.setUnique("UNI".equalsIgnoreCase(rs.getString("Key")));
                meta.setDefaultValue(rs.getString("Default"));
                meta.setExtra(rs.getString("Extra"));
                if (meta.isKey()) {
                    primaryKeys.add(meta.getColumnName());
                }
                fieldMetas.add(meta);
            }
            return new MySqlTableDefinition(
                            io.debezium.relational.TableId.parse(tableId.toString()),
                            fieldMetas,
                            primaryKeys)
                    .toDdl();

        } catch (SQLException e) {
            throw new RuntimeException(String.format("Failed to describe table %s", tableId), e);
        }
    }

    public static synchronized Table parseDdl(String ddlStatement, TableId tableId) {
        MySqlAntlrDdlParser mySqlAntlrDdlParser = getParser();
        mySqlAntlrDdlParser.setCurrentDatabase(tableId.getSchemaName());
        Tables tables = new Tables();
        mySqlAntlrDdlParser.parse(ddlStatement, tables);
        return tables.forTable(io.debezium.relational.TableId.parse(tableId.toString()));
    }

    public static synchronized MySqlAntlrDdlParser getParser() {
        if (mySqlAntlrDdlParser == null) {
            mySqlAntlrDdlParser = new MySqlAntlrDdlParser();
        }
        return mySqlAntlrDdlParser;
    }

    public static String showCreateTable(Connection jdbc, TableId tableId) {
        final String showCreateTableQuery =
                String.format(
                        "SHOW CREATE TABLE `%s`.`%s`",
                        tableId.getSchemaName(), tableId.getTableName());
        ResultSet rs = null;
        Statement st = null;
        try {
            st = jdbc.createStatement();
            rs = jdbc.createStatement().executeQuery(showCreateTableQuery);

            String ddlStatement = null;
            while (rs.next()) {
                ddlStatement = rs.getString(2);
            }
            return ddlStatement;
        } catch (SQLException e) {
            throw new RuntimeException(
                    String.format("Failed to show create table for %s", tableId), e);
        } finally {
            try {
                rs.close();
                st.close();
                jdbc.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static ResolvedSchema getResolvedSchema(TableId tableId, JdbcInfo jdbcInfo) {
        Connection connection = openJdbcConnection(jdbcInfo);
        String ddlStatement = showCreateTable(connection, tableId);
        Table table = parseDdl(ddlStatement, tableId);
        List<Column> columns = table.columns();
        Schema.Builder tableBuilder = Schema.newBuilder();
        List<org.apache.flink.table.catalog.Column> columns1 = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            org.apache.flink.table.catalog.Column column1 =
                    org.apache.flink.table.catalog.Column.physical(
                            column.name(), convertToFlinkTypeFromColumn(column));
            columns1.add(column1);
        }
        List<String> primaryKey = new ArrayList<>();
        primaryKey.addAll(table.primaryKeyColumnNames());
        if (Objects.nonNull(primaryKey) && !primaryKey.isEmpty()) {
            tableBuilder.primaryKey(primaryKey);
        }
        return new ResolvedSchema(
                columns1,
                new ArrayList<>(),
                UniqueConstraint.primaryKey("pk", primaryKey) // watermark，这里假设为 null
                );
    }

    private static org.apache.flink.table.types.DataType convertToFlinkTypeFromColumn(
            Column column) {
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
                if (column.length() != FLOAT_LENGTH_UNSPECIFIED_FLAG) {
                    // For FLOAT types with length provided explicitly, treat it like DOUBLE
                    return DataTypes.DOUBLE();
                } else {
                    return DataTypes.FLOAT();
                }
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
                        String.format("MySQL type '%s' is not supported yet.", typeName));
        }
    }

    public static List<com.ververica.cdc.common.event.TableId> listTables(
            JdbcInfo jdbcInfo, @Nullable String dbName) {
        try {
            List<String> databases =
                    dbName != null ? Collections.singletonList(dbName) : listDatabases(jdbcInfo);

            List<com.ververica.cdc.common.event.TableId> tableIds = new ArrayList<>();
            for (String database : databases) {
                tableIds.addAll(listTablesOfDatabase(jdbcInfo, database));
            }
            return tableIds;
        } catch (SQLException e) {
            throw new RuntimeException("Error to list databases: " + e.getMessage(), e);
        }
    }

    public static List<com.ververica.cdc.common.event.TableId> listTablesOfDatabase(
            JdbcInfo jdbcInfo, String dbName) throws SQLException {
        // ----------------
        // READ TABLE NAMES
        // ----------------
        // Get the list of table IDs for each database. We can't use a prepared statement with
        // tidb, so we have to build the SQL statement each time. Although in other cases this
        // might lead to SQL injection, in our case we are reading the database names from the
        // database and not taking them from the user ...
        LOG.info("Read list of available tables in {}", dbName);
        final List<com.ververica.cdc.common.event.TableId> tableIds = new ArrayList<>();
        Connection jdbc = SchemaUtils.openJdbcConnection(jdbcInfo);
        ResultSet rs = null;
        Statement st = null;
        try {
            st = jdbc.createStatement();
            rs =
                    jdbc.createStatement()
                            .executeQuery(
                                    "SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema = '"
                                            + dbName
                                            + "' ;");
            while (rs.next()) {
                tableIds.add(
                        com.ververica.cdc.common.event.TableId.tableId(
                                rs.getString(1), rs.getString(2)));
            }
            return tableIds;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to list tables", e);
        } finally {
            try {
                rs.close();
                st.close();
                jdbc.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static List<String> listDatabases(JdbcInfo jdbcInfo) throws SQLException {
        // -------------------
        // READ DATABASE NAMES
        // -------------------
        // Get the list of databases ...
        LOG.info("Read list of available databases");
        final List<String> databaseNames = new ArrayList<>();
        Connection jdbc = SchemaUtils.openJdbcConnection(jdbcInfo);

        ResultSet rs = null;
        Statement st = null;
        try {
            st = jdbc.createStatement();
            rs = jdbc.createStatement().executeQuery("show databases");
            while (rs.next()) {
                databaseNames.add(rs.getString(1));
            }
            LOG.info(" list of available databases are: {}", databaseNames);
            return databaseNames;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to show databases", e);
        } finally {
            try {
                rs.close();
                st.close();
                jdbc.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static List<TableId> getCapturedTableIds(List<String> tables, JdbcInfo jdbcInfo) {
        Selectors selectors =
                new Selectors.SelectorsBuilder().includeTables(String.join(", ", tables)).build();
        String[] capturedTables = getTableList(jdbcInfo, selectors);
        List<TableId> capturedTableIds = new ArrayList<>();
        for (String table : capturedTables) {
            TableId capturedTableId = TableId.parse(table);
            capturedTableIds.add(capturedTableId);
        }
        return capturedTableIds;
    }

    private static String[] getTableList(JdbcInfo jdbcInfo, Selectors selectors) {
        return SchemaUtils.listTables(jdbcInfo, null).stream()
                .filter(selectors::isMatch)
                .map(com.ververica.cdc.common.event.TableId::toString)
                .toArray(String[]::new);
    }
}
