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

import com.ververica.cdc.runtime.operators.sink.CustomRegistryAndReSendCreateTable;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.sink.CustomSink;
import com.ververica.cdc.common.sink.DataSink;
import com.ververica.cdc.connectors.iceberg.utils.IcebergUtils;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkCatalogFactory;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** A {@link DataSink} for "iceberg" connector. */
public class IcebergSink implements CustomSink<Event>, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(IcebergSink.class);
    Configuration config;
    private final ZoneId zoneId;

    public IcebergSink(Configuration config) {
        this(config, ZoneId.systemDefault());
    }

    public IcebergSink(Configuration config, ZoneId zoneId) {
        this.config = config;
        this.zoneId = zoneId;
    }

    @Override
    public void sinkTo(DataStream<Event> dataStream, OperatorID schemaOperatorId) {

        String catalogType = config.get(IcebergDataSinkOptions.CATALOG_TYPE);
        LOG.info("current use catalog type is {}", catalogType);
        switch (catalogType) {
            case FlinkCatalogFactory.ICEBERG_CATALOG_TYPE_HIVE:
                hiveCatalogWrite(dataStream, schemaOperatorId);
                break;
            case FlinkCatalogFactory.ICEBERG_CATALOG_TYPE_HADOOP:
                hadoopCatalogWrite(dataStream, schemaOperatorId);
                break;
            default:
                throw new RuntimeException("catalog type is invalid,please check!");
        }
    }

    private void hiveCatalogWrite(
            DataStream<Event> dataStream,
            OperatorID schemaOperatorId) {
        String database = config.getOptional(IcebergDataSinkOptions.DATABASE).get();
        String tableName = config.getOptional(IcebergDataSinkOptions.TABLENAME).get();
        String catalog = config.get(IcebergDataSinkOptions.HIVECATALOG);
        TableIdentifier identifier = TableIdentifier.of(Namespace.of(database), tableName);
        final CatalogLoader catalogLoader = IcebergUtils.catalogLoader(catalog, config);
        Table table = catalogLoader.loadCatalog().loadTable(identifier);
        int sinkParallelism = config.get(IcebergDataSinkOptions.SINK_PARALLELISM);
        final TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, identifier);
        Set<String> identifierFieldNameSet = table.schema().identifierFieldNames();
        List<String> identifierFieldNameList = new ArrayList<>();
        identifierFieldNameList.addAll(identifierFieldNameSet);
        DataStream<RowData> dStremMor = serializerDataStream(dataStream, table, schemaOperatorId);
        //   TODO 通过DataStream APi 往iceberg 写数据
        FlinkSink.forRowData(dStremMor)
                .writeParallelism(sinkParallelism)
                .table(table)
                .tableLoader(tableLoader)
                .tableSchema(FlinkSchemaUtil.toSchema(table.schema()))
                .equalityFieldColumns(identifierFieldNameList)
                .upsert(config.get(IcebergDataSinkOptions.UPSERT_MODE))
                .overwrite(config.get(IcebergDataSinkOptions.OVERWRITE_MODE))
                .append();
    }

    private DataStream<RowData> serializerDataStream(
            DataStream<Event> dataStream, Table table,
            OperatorID schemaOperatorId) {
        IcebergEventSerializer serializer =
                new IcebergEventSerializer(
                        zoneId, FlinkSchemaUtil.toSchema(table.schema()).getTableColumns());
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
        return dStremMor;
    }

    public void hadoopCatalogWrite(
            DataStream<Event> dataStream,
            OperatorID schemaOperatorId) {
        org.apache.hadoop.conf.Configuration conf = IcebergUtils.hadoopConfiguration(config);
        String warehouse = config.get(IcebergDataSinkOptions.WAREHOUSE);
        HadoopCatalog hadoopCatalog = new HadoopCatalog(conf, warehouse);
        String database = config.getOptional(IcebergDataSinkOptions.DATABASE).get();
        String tableName = config.getOptional(IcebergDataSinkOptions.TABLENAME).get();
        int sinkParallelism = config.getOptional(IcebergDataSinkOptions.SINK_PARALLELISM).get();
        TableIdentifier identifier = TableIdentifier.of(Namespace.of(database), tableName);
        Table table = hadoopCatalog.loadTable(identifier);
        Set<String> identifierFieldNameSet = table.schema().identifierFieldNames();
        List<String> identifierFieldNameList = new ArrayList<>();
        identifierFieldNameList.addAll(identifierFieldNameSet);
        final TableLoader tableLoader = TableLoader.fromHadoopTable(warehouse + tableName, conf);
        DataStream<RowData> dStremMor = serializerDataStream(dataStream, table, schemaOperatorId);
        FlinkSink.forRowData(dStremMor)
                .writeParallelism(sinkParallelism)
                .table(table)
                .tableLoader(tableLoader)
                .tableSchema(FlinkSchemaUtil.toSchema(table.schema()))
                .equalityFieldColumns(identifierFieldNameList)
                .upsert(config.get(IcebergDataSinkOptions.UPSERT_MODE))
                .overwrite(config.get(IcebergDataSinkOptions.OVERWRITE_MODE))
                .append();
    }
}
