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

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.source.MetadataAccessor;
import com.ververica.cdc.connectors.oracle.source.config.OracleSourceConfig;
import com.ververica.cdc.connectors.oracle.utils.OracleSchemaUtils;
import io.debezium.connector.oracle.OraclePartition;

import javax.annotation.Nullable;

import java.util.List;

/** {@link MetadataAccessor} for {@link OracleDataSource}. */
@Internal
public class OracleMetadataAccessor implements MetadataAccessor {

    private final OracleSourceConfig sourceConfig;

    private final OraclePartition partition;

    public OracleMetadataAccessor(OracleSourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
        this.partition = new OraclePartition(sourceConfig.getDbzConnectorConfig().getLogicalName());
    }

    /**
     * Always throw {@link UnsupportedOperationException} because oracle does not support namespace.
     */
    @Override
    public List<String> listNamespaces() {
        throw new UnsupportedOperationException("List namespace is not supported by oracle.");
    }

    /**
     * List all database from oracle.
     *
     * @param namespace This parameter is ignored because oracle does not support namespace.
     * @return The list of database
     */
    @Override
    public List<String> listSchemas(@Nullable String namespace) {
        return OracleSchemaUtils.listDatabases(sourceConfig);
    }

    /**
     * List tables from oracle.
     *
     * @param namespace This parameter is ignored because oracle does not support namespace.
     * @param dbName The database to list tables from. If null, list tables from all databases.
     * @return The list of {@link TableId}s.
     */
    @Override
    public List<TableId> listTables(@Nullable String namespace, @Nullable String dbName) {
        return OracleSchemaUtils.listTables(sourceConfig, dbName);
    }

    /**
     * Get the {@link Schema} of the given table.
     *
     * @param tableId The {@link TableId} of the given table.
     * @return The {@link Schema} of the table.
     */
    @Override
    public Schema getTableSchema(TableId tableId) {
        Schema schema = OracleSchemaUtils.getTableSchema(tableId, sourceConfig);
        return schema;
    }
}
