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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.util.Collector;

import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.sink.CustomSink;
import com.ververica.cdc.common.sink.DataSink;
import com.ververica.cdc.connectors.hudi.utils.HudiSInkUtils;
import com.ververica.cdc.runtime.operators.sink.CustomRegistryAndReSendCreateTable;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.org.apache.avro.LogicalTypes;
import org.apache.hudi.util.HoodiePipeline;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;

import static com.ververica.cdc.connectors.hudi.utils.HudiSInkUtils.getHoodieTableMetaClient;

/** A {@link DataSink} for "hudi" connector. */
public class HudiSink implements CustomSink<Event>, Serializable {
    Configuration config;
    private static final String COMM = ",";

    public HudiSink(Configuration config) {
        this.config = config;
    }

    @Override
    public void sinkTo(DataStream<Event> dataStream, OperatorID schemaOperatorId) {

        HoodieTableMetaClient hoodieTableMetaClient = getHoodieTableMetaClient(config);
        org.apache.hudi.org.apache.avro.Schema schema =
                hoodieTableMetaClient.getTableConfig().getTableCreateSchema().get();
        String tableType = hoodieTableMetaClient.getTableConfig().getTableType().name();
        List<org.apache.hudi.org.apache.avro.Schema.Field> fields = schema.getFields();
        String prefix = "option.pipeline.";
        HashMap<String, String> map = new HashMap<>();
        HudiSInkUtils.getConfigMap(map, prefix, config);
        HoodiePipeline.Builder builderMor =
                HoodiePipeline.builder(config.getOptional(HudiDataSinkOptions.TABLENAME).get())
                        .pk(config.getOptional(HudiDataSinkOptions.PRIMARYKEY).get())
                        .option(FlinkOptions.TABLE_TYPE.key(), tableType)
                        .option(
                                FlinkOptions.PATH.key(),
                                config.getOptional(HudiDataSinkOptions.TABLEPATH).get())
                        .option(
                                FlinkOptions.COMPACTION_TASKS.key(),
                                config.get(HudiDataSinkOptions.COMPACTION_TASKS_NUM))
                        .option(
                                FlinkOptions.WRITE_TASKS.key(),
                                config.get(HudiDataSinkOptions.WRITE_TASKS_NUM))
                        .option("hoodie.index.type", config.get(HudiDataSinkOptions.INDEX_TYPE))
                        .option(
                                "hoodie.cleaner.commits.retained",
                                config.get(HudiDataSinkOptions.CLEANER_COMMITS_RETAINED))
                        .option(
                                "hoodie.schema.on.read.enable",
                                config.get(HudiDataSinkOptions.SCHEMA_ON_READ_ENABLE))
                        .option(
                                "hoodie.datasource.write.reconcile.schema",
                                config.get(HudiDataSinkOptions.DATASOURCE_WRITE_RECONCILE_SCHEMA))
                        .option(
                                FlinkOptions.HIVE_SYNC_ENABLED.key(),
                                config.get(HudiDataSinkOptions.HIVE_SYNC_ENABLED))
                        .option(
                                FlinkOptions.READ_AS_STREAMING.key(),
                                config.get(HudiDataSinkOptions.READ_AS_STREAMING))
                        .option(
                                FlinkOptions.READ_STREAMING_CHECK_INTERVAL.key(),
                                config.get(HudiDataSinkOptions.READ_STREAMING_CHECK_INTERVAL))
                        .option(
                                FlinkOptions.OPERATION,
                                config.get(HudiDataSinkOptions.WRITE_OPERATION_MODE));
        for (Object key : map.keySet()) {
            builderMor.option(key.toString(), map.get(key));
        }
        for (org.apache.hudi.org.apache.avro.Schema.Field field : fields) {
            builderMor.column(AvroTypeUtil.toLogicalType(field));
        }
        if (config.getOptional(HudiDataSinkOptions.PARTITIONFIELD).isPresent()) {
            builderMor.partition(
                    config.getOptional(HudiDataSinkOptions.PARTITIONFIELD).get().split(COMM));
        }
        HudiEventSerializer serializer =
                new HudiEventSerializer(
                        ZoneId.of(config.get(HudiDataSinkOptions.SERVER_TIME_ZONE)));
        DataStream<RowData> dStremMor =
                dataStream
                        .transform(
                                "customRegistrerAndReCreate",
                                TypeInformation.of(Event.class),
                                new CustomRegistryAndReSendCreateTable(schemaOperatorId))
                        .flatMap(
                                new FlatMapFunction<Event, RowData>() {
                                    @Override
                                    public void flatMap(Event value, Collector<RowData> out)
                                            throws Exception {
                                        serializer.serialize(value, out);
                                    }
                                });
        dStremMor = dStremMor.filter(e -> e != null);
        builderMor.sink(dStremMor, false);
    }

    // Auxiliary class for converting Avro Schema to Flink LogicalType
    private static class AvroTypeUtil {
        public static String toLogicalType(org.apache.hudi.org.apache.avro.Schema.Field field) {
            String name = field.name(); // 获取字段名
            org.apache.hudi.org.apache.avro.Schema.Type type = field.schema().getType(); // 获取字段类型
            if (type.name().toLowerCase(Locale.ROOT).equals("union")) {
                type = field.schema().getTypes().get(1).getType();
            }
            StringBuilder result = new StringBuilder();
            // Convert the Avro type to the corresponding table type
            String typeName = "";
            switch (type) {
                case BOOLEAN:
                    typeName = LogicalTypeRoot.BOOLEAN.name();
                    break;
                case INT:
                    if (field.schema().toString().contains("date")) {
                        typeName = DataTypes.DATE().getLogicalType().getTypeRoot().name();
                    } else {
                        typeName = "INT";
                    }
                    break;
                case LONG:
                    if (field.schema().toString().contains("timestamp-micros")) {
                        typeName = "timestamp";
                    } else {
                        typeName = LogicalTypeRoot.BIGINT.name();
                    }
                    break;
                case FLOAT:
                    typeName = LogicalTypeRoot.FLOAT.name();
                    break;
                case DOUBLE:
                    typeName = LogicalTypeRoot.DOUBLE.name();
                    break;
                case STRING:
                    typeName = LogicalTypeRoot.VARCHAR.name();
                    break;
                case BYTES:
                    if (field.schema().getLogicalType() == LogicalTypes.decimal(10, 2)) {
                        typeName = DataTypes.DECIMAL(10, 2).getLogicalType().getTypeRoot().name();
                    } else {
                        typeName = LogicalTypeRoot.VARBINARY.name();
                    }
                    break;
                case NULL:
                    typeName = LogicalTypeRoot.NULL.name();
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported Avro type: " + type);
            }

            // Add fields to the table structure description
            return result.append(name).append(" ").append(typeName).toString();
        }
    }
}
