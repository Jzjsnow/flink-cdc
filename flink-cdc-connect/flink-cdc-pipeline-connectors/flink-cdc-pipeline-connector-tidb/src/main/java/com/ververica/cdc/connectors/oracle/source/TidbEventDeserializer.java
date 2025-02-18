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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.connectors.oracle.dto.JdbcInfo;
import com.ververica.cdc.connectors.oracle.utils.RowConvertUtils;
import com.ververica.cdc.connectors.oracle.utils.SchemaUtils;
import com.ververica.cdc.connectors.tidb.TiKVChangeEventDeserializationSchema;
import com.ververica.cdc.connectors.tidb.table.RowDataTiKVEventDeserializationSchemaBase;
import com.ververica.cdc.connectors.tidb.table.TiKVDeserializationRuntimeConverter;
import com.ververica.cdc.connectors.tidb.table.TiKVMetadataConverter;
import com.ververica.cdc.connectors.tidb.table.dto.TableInfo;
import org.tikv.common.TiConfiguration;
import org.tikv.common.key.RowKey;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.kvproto.Cdcpb;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.tikv.common.codec.TableCodec.decodeObjects;

/** Event deserializer for {@link TidbDataSource}. */
@Internal
public class TidbEventDeserializer extends RowDataTiKVEventDeserializationSchemaBase
        implements TiKVChangeEventDeserializationSchema<Event> {

    private static final long serialVersionUID = 1L;

    /** TypeInformation of the produced {@link RowData}. * */
    private final TypeInformation<Event> resultTypeInfo;

    private JdbcInfo jdbcInfo;
    private List<TableId> tables;

    private Map<Long, TableInfo> tableIdMap;

    public TidbEventDeserializer(
            TiConfiguration tiConf,
            TypeInformation<Event> resultTypeInfo,
            TiKVMetadataConverter[] metadataConverters,
            Map<String, RowType> physicalDataTypes,
            JdbcInfo jdbcInfo,
            List<TableId> tables) {
        super(tiConf, metadataConverters, physicalDataTypes, null);
        this.resultTypeInfo = checkNotNull(resultTypeInfo);
        this.jdbcInfo = jdbcInfo;
        this.tables = tables;
    }

    @Override
    public void deserialize(Cdcpb.Event.Row row, Collector<Event> out) throws Exception {
        if (tableIdMap == null) {
            tableIdMap = fetchTableIdMap(tables);
        }
        long tableIdNum = RowKey.decode(row.getKey().toByteArray()).getTableId();
        TableInfo tableDto = tableIdMap.get(tableIdNum);
        TiTableInfo tableInfo = tableDto.getTableInfo();
        final RowKey rowKey = RowKey.decode(row.getKey().toByteArray());
        final long handle = rowKey.getHandle();
        Object[] tikvValues;
        DataChangeEvent event;
        String database = tableDto.getTableName().split("\\.")[0];
        String tableName = tableDto.getTableName().split("\\.")[1];
        TableId tableId = TableId.tableId(database, tableName);
        com.ververica.cdc.common.schema.Schema schema =
                SchemaUtils.getSchema(database + "." + tableName, jdbcInfo);
        TiKVDeserializationRuntimeConverter physicalConverter =
                physicalConverterMap.get(database + "." + tableName);
        switch (row.getOpType()) {
            case DELETE:
                tikvValues = decodeObjects(row.getOldValue().toByteArray(), handle, tableInfo);
                RowData rowDataDelete =
                        (RowData) physicalConverter.convert(tikvValues, tableInfo, null);
                rowDataDelete.setRowKind(RowKind.DELETE);

                GenericRowData genericRowData =
                        (GenericRowData) physicalConverter.convert(tikvValues, tableInfo, null);
                event =
                        DataChangeEvent.deleteEvent(
                                tableId,
                                RowConvertUtils.convert(genericRowData, schema),
                                new HashMap<>());
                out.collect(event);
                break;
            case PUT:
                try {
                    tikvValues =
                            decodeObjects(
                                    row.getValue().toByteArray(),
                                    RowKey.decode(row.getKey().toByteArray()).getHandle(),
                                    tableInfo);
                    if (row.getOldValue() == null || row.getOldValue().isEmpty()) {

                        genericRowData =
                                (GenericRowData)
                                        physicalConverter.convert(tikvValues, tableInfo, null);

                        event =
                                DataChangeEvent.insertEvent(
                                        tableId,
                                        RowConvertUtils.convert(genericRowData, schema),
                                        new HashMap<>());
                        out.collect(event);
                    } else {
                        Object[] tikvOldValues =
                                decodeObjects(row.getOldValue().toByteArray(), handle, tableInfo);
                        GenericRowData before =
                                (GenericRowData)
                                        physicalConverter.convert(tikvOldValues, tableInfo, null);

                        RecordData beforeRecord = RowConvertUtils.convert(before, schema);
                        GenericRowData alter =
                                (GenericRowData)
                                        physicalConverter.convert(tikvValues, tableInfo, null);
                        RecordData afterRecord = RowConvertUtils.convert(alter, schema);
                        event =
                                DataChangeEvent.updateEvent(
                                        tableId, beforeRecord, afterRecord, new HashMap<>());
                        out.collect(event);
                    }
                    break;
                } catch (final RuntimeException e) {
                    throw new FlinkRuntimeException(
                            String.format(
                                    "Fail to deserialize row: %s, table: %s",
                                    row, tableInfo.getId()),
                            e);
                }
            default:
                throw new IllegalArgumentException("Unknown Row Op Type: " + row.getOpType());
        }
    }

    @Override
    public TypeInformation getProducedType() {
        return resultTypeInfo;
    }
}
