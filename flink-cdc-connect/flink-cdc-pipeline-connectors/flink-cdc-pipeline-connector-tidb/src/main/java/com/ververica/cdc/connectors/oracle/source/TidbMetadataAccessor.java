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

package com.ververica.cdc.connectors.oracle.source;

import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.source.MetadataAccessor;
import com.ververica.cdc.connectors.oracle.dto.JdbcInfo;
import com.ververica.cdc.connectors.oracle.utils.SchemaUtils;

import javax.annotation.Nullable;

import java.sql.SQLException;
import java.util.List;

/** {@link MetadataAccessor} for {@link TidbDataSource}. */
@Internal
public class TidbMetadataAccessor implements MetadataAccessor {

    private JdbcInfo jdbcInfo;

    public TidbMetadataAccessor(Configuration config) {
        String hostname = config.get(TidbDataSourceOptions.HOSTNAME);
        int tidbPort = config.get(TidbDataSourceOptions.TIDB_PORT);
        String username = config.get(TidbDataSourceOptions.USERNAME);
        String password = config.get(TidbDataSourceOptions.PASSWORD);

        jdbcInfo = new JdbcInfo();
        jdbcInfo.setPassword(password);
        jdbcInfo.setDatabase("INFORMATION_SCHEMA");
        jdbcInfo.setHost(hostname);
        jdbcInfo.setPort(tidbPort);
        jdbcInfo.setUsername(username);
    }

    /**
     * Always throw {@link UnsupportedOperationException} because tidb does not support namespace.
     */
    @Override
    public List<String> listNamespaces() {
        throw new UnsupportedOperationException("List namespace is not supported by tidb.");
    }

    /**
     * List all database from tidb.
     *
     * @param namespace This parameter is ignored because tidb does not support namespace.
     * @return The list of database
     */
    @Override
    public List<String> listSchemas(@Nullable String namespace) {
        try {
            return SchemaUtils.listDatabases(jdbcInfo);
        } catch (SQLException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    /**
     * List tables from tidb.
     *
     * @param namespace This parameter is ignored because tidb does not support namespace.
     * @param dbName The database to list tables from. If null, list tables from all databases.
     * @return The list of {@link TableId}s.
     */
    @Override
    public List<TableId> listTables(@Nullable String namespace, @Nullable String dbName) {
        return SchemaUtils.listTables(jdbcInfo, dbName);
    }

    /**
     * Get the {@link Schema} of the given table.
     *
     * @param tableId The {@link TableId} of the given table.
     * @return The {@link Schema} of the table.
     */
    @Override
    public Schema getTableSchema(TableId tableId) {
        return SchemaUtils.getSchema(tableId.toString(), jdbcInfo);
    }
}
