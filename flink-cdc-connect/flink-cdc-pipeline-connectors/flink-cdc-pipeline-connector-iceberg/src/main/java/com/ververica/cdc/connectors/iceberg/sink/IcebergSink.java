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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.common.sink.CustomSink;
import com.ververica.cdc.common.sink.DataSink;
import com.ververica.cdc.connectors.iceberg.utils.IcebergUtils;
import com.ververica.cdc.runtime.operators.sink.CustomRegistryAndReSendCreateTable;
import org.apache.commons.lang3.StringUtils;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    private void hiveCatalogWrite(DataStream<Event> dataStream, OperatorID schemaOperatorId) {
        String database = IcebergUtils.getDatabase(config);
        List<String> tables = IcebergUtils.getTables(config);
        String catalog = config.get(IcebergDataSinkOptions.CATALOG_NAME);
        String uidPrefix = config.get(IcebergDataSinkOptions.UID_PREFIX);
        final CatalogLoader catalogLoader = IcebergUtils.catalogLoader(catalog, config);
        int sinkParallelism = config.get(IcebergDataSinkOptions.SINK_PARALLELISM);

        buildIcebergSinks(
                dataStream,
                schemaOperatorId,
                uidPrefix,
                sinkParallelism,
                database,
                tables,
                catalogLoader);
    }

    private DataStream<RowData> serializerDataStream(DataStream<Event> dataStream, Table table) {
        IcebergEventSerializer serializer =
                new IcebergEventSerializer(
                        zoneId, FlinkSchemaUtil.toSchema(table.schema()).getTableColumns());
        return dataStream.flatMap(
                new FlatMapFunction<Event, RowData>() {
                    @Override
                    public void flatMap(Event value, Collector<RowData> out) throws Exception {
                        serializer.serialize(value, out);
                    }
                });
    }

    private List<DataStream<RowData>> serializerDataStreams(
            DataStream<Event> dataStream,
            List<Tuple2<TableIdentifier, Table>> tables,
            OperatorID schemaOperatorId) {

        DataStream<Event> totalDataStream =
                dataStream.transform(
                        "customRegistrerAndReCreate",
                        TypeInformation.of(Event.class),
                        new CustomRegistryAndReSendCreateTable(schemaOperatorId));
        List<DataStream<RowData>> dStremMors = new ArrayList<>();

        if (tables.size() == 1) {
            LOG.info("Create a single sink for table {}", tables.get(0).f0);
            dStremMors.add(serializerDataStream(totalDataStream, tables.get(0).f1));
            return dStremMors;
        }

        LOG.info("Create multiple sinks for tables {}", tables);
        String mainStreamTableIdentifier = tables.get(0).f0.toString().trim();
        Table mainStreamTable = tables.get(0).f1;

        Map<String, OutputTag<Event>> sideOutputTableMap = new HashMap<>();
        for (int i = 1; i < tables.size(); i++) {
            String tableIdentifier = tables.get(i).f0.toString().trim();
            final OutputTag<Event> outputTag = new OutputTag<Event>(tableIdentifier) {};
            sideOutputTableMap.put(tableIdentifier, outputTag);
        }

        SingleOutputStreamOperator<Event> mainDataStream =
                totalDataStream.process(
                        new ProcessFunction<Event, Event>() {

                            @Override
                            public void processElement(
                                    Event event, Context ctx, Collector<Event> out) {

                                String tableIdentifier;
                                if (event instanceof DataChangeEvent) {
                                    DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
                                    tableIdentifier = dataChangeEvent.tableId().identifier();
                                } else if (event instanceof SchemaChangeEvent) {
                                    SchemaChangeEvent schemaChangeEvent = (SchemaChangeEvent) event;
                                    tableIdentifier = schemaChangeEvent.tableId().identifier();
                                } else {
                                    throw new IllegalArgumentException(
                                            "Unsupported Event type: " + event.getClass());
                                }

                                if (tableIdentifier.equals(mainStreamTableIdentifier)) {
                                    out.collect(event);
                                } else if (sideOutputTableMap.containsKey(tableIdentifier)) {
                                    // emit data to side output
                                    ctx.output(sideOutputTableMap.get(tableIdentifier), event);
                                } else {
                                    LOG.error("Unknown table: {}", tableIdentifier);
                                    throw new IllegalArgumentException(
                                            "Unknown table: " + tableIdentifier);
                                }
                            }
                        });

        tables.forEach(
                (tableTuple2) -> {
                    String tableIdentifier = tableTuple2.f0.toString().trim();
                    if (tableIdentifier.equals(mainStreamTableIdentifier)) {
                        LOG.info("Set table {} to main stream", mainStreamTableIdentifier);
                        dStremMors.add(serializerDataStream(mainDataStream, mainStreamTable));
                    } else if (sideOutputTableMap.containsKey(tableIdentifier)) {
                        DataStream<Event> sideOutputStream =
                                mainDataStream.getSideOutput(
                                        sideOutputTableMap.get(tableIdentifier));
                        LOG.info("Set table {} to side output stream", tableIdentifier);
                        dStremMors.add(serializerDataStream(sideOutputStream, tableTuple2.f1));
                    } else {
                        LOG.error("No side output stream for table: {}", tableIdentifier);
                        throw new IllegalArgumentException(
                                "No side output stream for table: " + tableIdentifier);
                    }
                });

        return dStremMors;
    }

    public void hadoopCatalogWrite(DataStream<Event> dataStream, OperatorID schemaOperatorId) {
        org.apache.hadoop.conf.Configuration conf = IcebergUtils.hadoopConfiguration(config);
        String warehouse = config.get(IcebergDataSinkOptions.WAREHOUSE);
        HadoopCatalog hadoopCatalog = new HadoopCatalog(conf, warehouse);
        String database = IcebergUtils.getDatabase(config);
        List<String> tables = IcebergUtils.getTables(config);
        String uidPrefix = config.get(IcebergDataSinkOptions.UID_PREFIX);
        int sinkParallelism = config.get(IcebergDataSinkOptions.SINK_PARALLELISM);

        buildIcebergSinks(
                dataStream,
                schemaOperatorId,
                uidPrefix,
                sinkParallelism,
                warehouse,
                hadoopCatalog,
                database,
                tables,
                conf);
    }

    private void buildIcebergSink(
            DataStream<RowData> dStremMor,
            String uidPrefix,
            int sinkParallelism,
            TableIdentifier identifier,
            Table table,
            List<String> identifierFieldNameList,
            TableLoader tableLoader) {
        FlinkSink.Builder builder =
                FlinkSink.forRowData(dStremMor)
                        .writeParallelism(sinkParallelism)
                        .table(table)
                        .tableLoader(tableLoader)
                        .tableSchema(FlinkSchemaUtil.toSchema(table.schema()))
                        .equalityFieldColumns(identifierFieldNameList)
                        .upsert(config.get(IcebergDataSinkOptions.UPSERT_MODE))
                        .overwrite(config.get(IcebergDataSinkOptions.OVERWRITE_MODE));
        if (StringUtils.isNotBlank(uidPrefix)) {
            builder.uidPrefix(uidPrefix + "_" + identifier.name());
        }
        builder.append();
    }

    private void buildIcebergSinks(
            DataStream<Event> dataStream,
            OperatorID schemaOperatorId,
            String uidPrefix,
            int sinkParallelism,
            String database,
            List<String> tableNames,
            CatalogLoader catalogLoader) {

        List<Tuple2<TableIdentifier, Table>> tables = new ArrayList<>();
        for (String tableName : tableNames) {
            TableIdentifier identifier = TableIdentifier.of(Namespace.of(database), tableName);
            tables.add(Tuple2.of(identifier, catalogLoader.loadCatalog().loadTable(identifier)));
        }

        if (tables.isEmpty()) {
            throw new IllegalArgumentException("No tables found for building Iceberg Sinks");
        }

        List<DataStream<RowData>> dStremMors =
                serializerDataStreams(dataStream, tables, schemaOperatorId);
        for (int i = 0; i < dStremMors.size(); i++) {
            DataStream<RowData> dStremMor = dStremMors.get(i);
            TableIdentifier identifier = tables.get(i).f0;
            Table table = tables.get(i).f1;
            final TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, identifier);
            Set<String> identifierFieldNameSet = table.schema().identifierFieldNames();
            List<String> identifierFieldNameList = new ArrayList<>(identifierFieldNameSet);
            buildIcebergSink(
                    dStremMor,
                    uidPrefix,
                    sinkParallelism,
                    identifier,
                    table,
                    identifierFieldNameList,
                    tableLoader);
        }
    }

    private void buildIcebergSinks(
            DataStream<Event> dataStream,
            OperatorID schemaOperatorId,
            String uidPrefix,
            int sinkParallelism,
            String warehouse,
            HadoopCatalog hadoopCatalog,
            String database,
            List<String> tableNames,
            org.apache.hadoop.conf.Configuration conf) {

        List<Tuple2<TableIdentifier, Table>> tables = new ArrayList<>();
        for (String tableName : tableNames) {
            TableIdentifier identifier = TableIdentifier.of(Namespace.of(database), tableName);
            tables.add(Tuple2.of(identifier, hadoopCatalog.loadTable(identifier)));
        }

        if (tables.isEmpty()) {
            throw new IllegalArgumentException("No tables found for building Iceberg Sinks");
        }

        List<DataStream<RowData>> dStremMors =
                serializerDataStreams(dataStream, tables, schemaOperatorId);
        for (int i = 0; i < dStremMors.size(); i++) {
            DataStream<RowData> dStremMor = dStremMors.get(i);
            TableIdentifier identifier = tables.get(i).f0;
            Table table = tables.get(i).f1;
            final TableLoader tableLoader =
                    TableLoader.fromHadoopTable(warehouse + identifier.name(), conf);
            Set<String> identifierFieldNameSet = table.schema().identifierFieldNames();
            List<String> identifierFieldNameList = new ArrayList<>(identifierFieldNameSet);
            buildIcebergSink(
                    dStremMor,
                    uidPrefix,
                    sinkParallelism,
                    identifier,
                    table,
                    identifierFieldNameList,
                    tableLoader);
        }
    }
}
