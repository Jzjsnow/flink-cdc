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

package com.ververica.cdc.connectors.iceberg.sink;

import com.ververica.cdc.common.event.AddColumnEvent;
import com.ververica.cdc.common.event.AlterColumnTypeEvent;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.DropColumnEvent;
import com.ververica.cdc.common.event.RenameColumnEvent;
import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.sink.MetadataApplier;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.IntType;
import com.ververica.cdc.common.types.LocalZonedTimestampType;
import com.ververica.cdc.common.types.TimestampType;
import com.ververica.cdc.common.types.VarCharType;
import com.ververica.cdc.common.types.ZonedTimestampType;
import com.ververica.cdc.connectors.iceberg.utils.IcebergUtils;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkCatalogFactory;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/** Supports {@link IcebergDataSink} to schema evolution. */
public class IcebergMetadataApplier implements MetadataApplier {
    private static final Logger LOG = LoggerFactory.getLogger(IcebergMetadataApplier.class);
    private com.ververica.cdc.common.configuration.Configuration config;

    public IcebergMetadataApplier(com.ververica.cdc.common.configuration.Configuration config) {
        this.config = config;
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent event) {
        try {
            String catalogType = config.get(IcebergDataSinkOptions.CATALOG_TYPE);
            if (!catalogType.equals(FlinkCatalogFactory.ICEBERG_CATALOG_TYPE_HIVE)) {
                LOG.info(
                        "catalog type is not hive,only hive catalog support schema change,current type is {}",
                        catalogType);
                return;
            }
            HiveCatalog catalog;
            final CatalogLoader catalogLoader =
                    IcebergUtils.catalogLoader(
                            config.getOptional(IcebergDataSinkOptions.CATALOG_NAME).get(), config);
            catalog = (HiveCatalog) catalogLoader.loadCatalog();
            Table table =
                    catalog.loadTable(
                            TableIdentifier.of(
                                    config.getOptional(IcebergDataSinkOptions.DATABASE).get(),
                                    config.getOptional(IcebergDataSinkOptions.TABLENAME).get()));
            UpdateSchema newSchema = table.updateSchema();
            // send schema change op to doris
            if (event instanceof CreateTableEvent) {
                LOG.info("it's not support table automatic creation now!");
                return;
            } else if (event instanceof AddColumnEvent) {
                LOG.info(
                        "table schema change: add column operation!columns={}",
                        ((AddColumnEvent) event).getAddedColumns());
                applyAddColumnEvent((AddColumnEvent) event, newSchema);
            } else if (event instanceof DropColumnEvent) {
                LOG.info(
                        "table schema change: drop column operation!columns={}",
                        ((DropColumnEvent) event).getDroppedColumns());
                applyDropColumnEvent((DropColumnEvent) event, newSchema);
            } else if (event instanceof RenameColumnEvent) {
                LOG.info(
                        "table schema change: rename column operation!columns={}",
                        ((RenameColumnEvent) event).getNameMapping());
                applyRenameColumnEvent((RenameColumnEvent) event, newSchema);
            } else if (event instanceof AlterColumnTypeEvent) {
                LOG.info(
                        "table schema change: drop column operation!columns={}",
                        ((AlterColumnTypeEvent) event).getTypeMapping());
                applyAlterColumnTypeEvent((AlterColumnTypeEvent) event, newSchema);
            } else {
                LOG.info("it's not support this table schema change operation!");
            }
        } catch (Exception ex) {
            throw new RuntimeException("Failed to schema change, " + event + ", reason: " + ex);
        }
    }

    private void applyAddColumnEvent(AddColumnEvent event, UpdateSchema newSchema)
            throws IOException, IllegalArgumentException {
        List<AddColumnEvent.ColumnWithPosition> addedColumns = event.getAddedColumns();
        for (AddColumnEvent.ColumnWithPosition col : addedColumns) {
            Column column = col.getAddColumn();
            if (column.getType() instanceof LocalZonedTimestampType
                    || column.getType() instanceof TimestampType
                    || column.getType() instanceof ZonedTimestampType) {
                newSchema.addColumn(column.getName(), Types.TimestampType.withZone()).commit();
            } else if (column.getType() instanceof VarCharType) {
                newSchema.addColumn(column.getName(), Types.StringType.get()).commit();
            } else if (column.getType() instanceof IntType) {
                newSchema.addColumn(column.getName(), Types.IntegerType.get()).commit();
            } else {
                newSchema.addColumn(column.getName(), Types.StringType.get()).commit();
            }
        }
    }

    private void applyDropColumnEvent(DropColumnEvent event, UpdateSchema newSchema)
            throws IOException, IllegalArgumentException {
        List<Column> droppedColumns = event.getDroppedColumns();
        for (Column col : droppedColumns) {
            newSchema.deleteColumn(col.getName()).commit();
        }
    }

    private void applyRenameColumnEvent(RenameColumnEvent event, UpdateSchema newSchema)
            throws IOException, IllegalArgumentException {
        Map<String, String> nameMapping = event.getNameMapping();
        for (String key : nameMapping.keySet()) {
            newSchema.renameColumn(key, nameMapping.get(key)).commit();
        }
    }

    private void applyAlterColumnTypeEvent(AlterColumnTypeEvent event, UpdateSchema newSchema)
            throws IOException, IllegalArgumentException {
        Map<String, DataType> dataTypeMapping = event.getTypeMapping();
        for (String key : dataTypeMapping.keySet()) {
            DataType type = dataTypeMapping.get(key);
            if (type instanceof LocalZonedTimestampType
                    || type instanceof TimestampType
                    || type instanceof ZonedTimestampType) {
                newSchema.updateColumn(key, Types.TimestampType.withZone()).commit();
            } else if (type instanceof VarCharType) {
                newSchema.updateColumn(key, Types.StringType.get()).commit();
            } else if (type instanceof IntType) {
                newSchema.updateColumn(key, Types.IntegerType.get()).commit();
            } else {
                newSchema.updateColumn(key, Types.StringType.get()).commit();
            }
        }
    }
}
