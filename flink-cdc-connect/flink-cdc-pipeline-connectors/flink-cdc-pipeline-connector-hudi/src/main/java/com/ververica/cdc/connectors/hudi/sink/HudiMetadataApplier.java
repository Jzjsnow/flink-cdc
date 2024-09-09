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

package com.ververica.cdc.connectors.hudi.sink;

import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.Preconditions;

import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.event.AddColumnEvent;
import com.ververica.cdc.common.event.AlterColumnTypeEvent;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.DropColumnEvent;
import com.ververica.cdc.common.event.RenameColumnEvent;
import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.sink.MetadataApplier;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DoubleType;
import com.ververica.cdc.common.types.LocalZonedTimestampType;
import com.ververica.cdc.common.types.TimestampType;
import com.ververica.cdc.common.types.VarCharType;
import com.ververica.cdc.common.types.ZonedTimestampType;
import com.ververica.cdc.connectors.hudi.utils.HudiSInkUtils;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.keygen.ComplexAvroKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.org.apache.avro.Schema;
import org.apache.hudi.org.apache.avro.SchemaBuilder;
import org.apache.hudi.table.HoodieTableFactory;
import org.apache.hudi.util.FlinkWriteClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.internal.schema.action.TableChange.ColumnPositionChange.ColumnPositionType.AFTER;

/** Supports {@link HudiDataSink} to schema evolution. */
public class HudiMetadataApplier implements MetadataApplier {
    private static final Logger LOG = LoggerFactory.getLogger(HudiMetadataApplier.class);
    private Configuration config;

    public HudiMetadataApplier(Configuration config) {
        this.config = config;
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent event) {
        try {
            HoodieTableMetaClient hoodieTableMetaClient =
                    HudiSInkUtils.getHoodieTableMetaClient(config);
            Schema oldSchema = hoodieTableMetaClient.getTableConfig().getTableCreateSchema().get();
            TableOptions tableOptions = getTableOptions(oldSchema);
            // send schema change op to doris
            if (event instanceof CreateTableEvent) {
                LOG.info("it's not support table automatic creation now!");
            } else if (event instanceof AddColumnEvent) {
                LOG.info(
                        "table schema change: add column operation!columns={}",
                        ((AddColumnEvent) event).getAddedColumns());
                applyAddColumnEvent((AddColumnEvent) event, tableOptions, oldSchema);
            } else if (event instanceof DropColumnEvent) {
                LOG.info(
                        "table schema change: drop column operation!columns={}",
                        ((DropColumnEvent) event).getDroppedColumns());
                applyDropColumnEvent((DropColumnEvent) event, tableOptions);
            } else if (event instanceof RenameColumnEvent) {
                LOG.info(
                        "table schema change: rename column operation!columns={}",
                        ((RenameColumnEvent) event).getNameMapping());
                applyRenameColumnEvent((RenameColumnEvent) event, tableOptions);
            } else if (event instanceof AlterColumnTypeEvent) {
                LOG.info(
                        "table schema change: alter column type operation!columns={}",
                        ((AlterColumnTypeEvent) event).getTypeMapping());
                applyAlterColumnTypeEvent((AlterColumnTypeEvent) event, tableOptions);
            } else {
                throw new RuntimeException("Unsupport schema change event, " + event);
            }
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Failed to schema change, " + event + ", reason: " + ex.getMessage());
        }
    }

    private void applyAddColumnEvent(AddColumnEvent event, TableOptions tableOptions, Schema schema)
            throws IOException {
        changeTableSchema(tableOptions, false, event, schema);
    }

    private TableOptions getTableOptions(Schema oldSchema) {
        TableOptions tableOptions = defaultTableOptions(oldSchema);
        return tableOptions;
    }

    private void changeTableSchema(
            TableOptions tableOptions,
            boolean shouldCompactBeforeSchemaChanges,
            AddColumnEvent event,
            Schema schema)
            throws IOException {
        try (HoodieFlinkWriteClient<?> writeClient =
                FlinkWriteClients.createWriteClient(tableOptions.toConfig())) {
            if (shouldCompactBeforeSchemaChanges) {
                Option<String> compactionInstant = writeClient.scheduleCompaction(Option.empty());
                writeClient.compact(compactionInstant.get());
            }
            Schema dataType;

            List<AddColumnEvent.ColumnWithPosition> addedColumns = event.getAddedColumns();
            for (AddColumnEvent.ColumnWithPosition col : addedColumns) {
                Column column = col.getAddColumn();
                if (column.getType() instanceof LocalZonedTimestampType
                        || column.getType() instanceof TimestampType
                        || column.getType() instanceof ZonedTimestampType) {
                    dataType = SchemaBuilder.unionOf().nullType().and().longType().endUnion();
                } else if (column.getType() instanceof DoubleType) {
                    dataType = SchemaBuilder.unionOf().nullType().and().doubleType().endUnion();
                } else if (column.getType() instanceof VarCharType) {
                    dataType = SchemaBuilder.unionOf().nullType().and().stringType().endUnion();
                } else {
                    dataType = SchemaBuilder.unionOf().nullType().and().stringType().endUnion();
                }
                List<Schema.Field> fields = schema.getFields();
                int index = fields.size();
                String position = fields.get(index - 1).name();
                writeClient.addColumn(
                        column.getName(), dataType, column.getComment(), position, AFTER);
            }
        }
    }

    public static HoodieTableMetaClient getMetaClient(
            org.apache.hadoop.conf.Configuration conf, String basePath) {
        return HoodieTableMetaClient.builder()
                .setConf(conf)
                .setBasePath(basePath)
                .setLoadActiveTimelineOnLoad(true)
                .build();
    }

    private void applyDropColumnEvent(DropColumnEvent event, TableOptions tableOptions)
            throws IOException {

        try (HoodieFlinkWriteClient<?> writeClient =
                FlinkWriteClients.createWriteClient(tableOptions.toConfig())) {
            List<Column> droppedColumns = event.getDroppedColumns();
            for (Column col : droppedColumns) {
                writeClient.deleteColumns(col.getName());
            }
        }
    }

    private void applyAlterColumnTypeEvent(AlterColumnTypeEvent event, TableOptions tableOptions)
            throws IOException {

        try (HoodieFlinkWriteClient<?> writeClient =
                FlinkWriteClients.createWriteClient(tableOptions.toConfig())) {
            Map<String, DataType> alterColumnTypeColumns = event.getTypeMapping();
            Type dataType = null;
            for (String key : alterColumnTypeColumns.keySet()) {
                DataType sourceDataType = alterColumnTypeColumns.get(key);
                if (sourceDataType instanceof LocalZonedTimestampType
                        || sourceDataType instanceof TimestampType
                        || sourceDataType instanceof ZonedTimestampType) {
                    dataType = Types.LongType.get();
                } else if (sourceDataType instanceof DoubleType) {
                    dataType = Types.DoubleType.get();
                } else if (sourceDataType instanceof VarCharType) {
                    dataType = Types.StringType.get();
                } else {
                    dataType = Types.StringType.get();
                }
                writeClient.updateColumnType(key, dataType);
            }
        }
    }

    private void applyRenameColumnEvent(RenameColumnEvent event, TableOptions tableOptions)
            throws IOException {
        try (HoodieFlinkWriteClient<?> writeClient =
                FlinkWriteClients.createWriteClient(tableOptions.toConfig())) {
            Map<String, String> nameMapping = event.getNameMapping();
            for (String key : nameMapping.keySet()) {
                writeClient.renameColumn(key, nameMapping.get(key));
            }
        }
    }

    private static final class TableOptions {
        private final Map<String, String> map = new HashMap<>();

        TableOptions(Object... options) {
            Preconditions.checkArgument(options.length % 2 == 0);
            for (int i = 0; i < options.length; i += 2) {
                withOption(options[i].toString(), options[i + 1]);
            }
        }

        TableOptions withOption(String optionName, Object optionValue) {
            if (StringUtils.isNullOrEmpty(optionName)) {
                throw new java.lang.IllegalArgumentException("optionName must be presented");
            }
            map.put(optionName, optionValue.toString());
            return this;
        }

        org.apache.flink.configuration.Configuration toConfig() {
            return FlinkOptions.fromMap(map);
        }

        @Override
        public String toString() {
            return map.entrySet().stream()
                    .map(e -> String.format("'%s' = '%s'", e.getKey(), e.getValue()))
                    .collect(Collectors.joining(", "));
        }
    }

    private TableOptions defaultTableOptions(Schema schema) {
        String tablePath = config.getOptional(HudiDataSinkOptions.TABLEPATH).get();
        String tableName = config.getOptional(HudiDataSinkOptions.TABLENAME).get();
        String pk = config.getOptional(HudiDataSinkOptions.PRIMARYKEY).get();
        String partitionField =
                config.getOptional(HudiDataSinkOptions.PARTITIONFIELD).isPresent()
                        ? config.getOptional(HudiDataSinkOptions.PARTITIONFIELD).get()
                        : null;
        return partitionField != null
                ? new TableOptions(
                        FactoryUtil.CONNECTOR.key(),
                        HoodieTableFactory.FACTORY_ID,
                        FlinkOptions.PATH.key(),
                        tablePath,
                        FlinkOptions.TABLE_TYPE.key(),
                        FlinkOptions.TABLE_TYPE_COPY_ON_WRITE,
                        HoodieTableConfig.NAME.key(),
                        tableName,
                        FlinkOptions.READ_AS_STREAMING.key(),
                        false,
                        FlinkOptions.QUERY_TYPE.key(),
                        FlinkOptions.QUERY_TYPE_SNAPSHOT,
                        KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(),
                        pk,
                        KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(),
                        partitionField,
                        KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE.key(),
                        true,
                        HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(),
                        ComplexAvroKeyGenerator.class.getName(),
                        FlinkOptions.WRITE_BATCH_SIZE.key(),
                        0.000001, // each record triggers flush
                        FlinkOptions.SOURCE_AVRO_SCHEMA.key(),
                        schema,
                        FlinkOptions.READ_TASKS.key(),
                        1,
                        FlinkOptions.WRITE_TASKS.key(),
                        1,
                        FlinkOptions.INDEX_BOOTSTRAP_TASKS.key(),
                        1,
                        FlinkOptions.BUCKET_ASSIGN_TASKS.key(),
                        1,
                        FlinkOptions.COMPACTION_TASKS.key(),
                        1,
                        FlinkOptions.COMPACTION_SCHEDULE_ENABLED.key(),
                        false,
                        HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key(),
                        true)
                : new TableOptions(
                        FactoryUtil.CONNECTOR.key(),
                        HoodieTableFactory.FACTORY_ID,
                        FlinkOptions.PATH.key(),
                        tablePath,
                        FlinkOptions.TABLE_TYPE.key(),
                        FlinkOptions.TABLE_TYPE_COPY_ON_WRITE,
                        HoodieTableConfig.NAME.key(),
                        tableName,
                        FlinkOptions.READ_AS_STREAMING.key(),
                        false,
                        FlinkOptions.QUERY_TYPE.key(),
                        FlinkOptions.QUERY_TYPE_SNAPSHOT,
                        KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(),
                        pk,
                        KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE.key(),
                        true,
                        HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(),
                        ComplexAvroKeyGenerator.class.getName(),
                        FlinkOptions.WRITE_BATCH_SIZE.key(),
                        0.000001, // each record triggers flush
                        FlinkOptions.SOURCE_AVRO_SCHEMA.key(),
                        schema,
                        FlinkOptions.READ_TASKS.key(),
                        1,
                        FlinkOptions.WRITE_TASKS.key(),
                        1,
                        FlinkOptions.INDEX_BOOTSTRAP_TASKS.key(),
                        1,
                        FlinkOptions.BUCKET_ASSIGN_TASKS.key(),
                        1,
                        FlinkOptions.COMPACTION_TASKS.key(),
                        1,
                        FlinkOptions.COMPACTION_SCHEDULE_ENABLED.key(),
                        false,
                        HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key(),
                        true);
    }
}
