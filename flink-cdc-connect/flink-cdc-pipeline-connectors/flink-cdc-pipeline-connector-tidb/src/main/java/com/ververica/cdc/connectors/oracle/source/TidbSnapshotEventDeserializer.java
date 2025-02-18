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
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.connectors.oracle.dto.JdbcInfo;
import com.ververica.cdc.connectors.oracle.utils.RowConvertUtils;
import com.ververica.cdc.connectors.oracle.utils.SchemaUtils;
import com.ververica.cdc.connectors.tidb.TiKVSnapshotEventDeserializationSchema;
import com.ververica.cdc.connectors.tidb.table.RowDataTiKVEventDeserializationSchemaBase;
import com.ververica.cdc.connectors.tidb.table.TiKVDeserializationRuntimeConverter;
import com.ververica.cdc.connectors.tidb.table.TiKVMetadataConverter;
import com.ververica.cdc.connectors.tidb.table.dto.TableInfo;
import com.ververica.cdc.debezium.table.DeserializationRuntimeConverter;
import org.tikv.common.TiConfiguration;
import org.tikv.common.key.RowKey;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.kvproto.Kvrpcpb;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.tikv.common.codec.TableCodec.decodeObjects;

/** Event deserializer for {@link TidbDataSource}. */
@Internal
public class TidbSnapshotEventDeserializer extends RowDataTiKVEventDeserializationSchemaBase
        implements TiKVSnapshotEventDeserializationSchema<Event> {

    private static final long serialVersionUID = 1L;

    private final TypeInformation<Event> resultTypeInfo;
    private static final Map<DataType, DeserializationRuntimeConverter> CONVERTERS =
            new ConcurrentHashMap<>();

    private final String sourceTimeZone;
    private JdbcInfo jdbcInfo;
    private Map<Long, TableInfo> tableIdMap;
    private List<TableId> tables;

    public TidbSnapshotEventDeserializer(
            TiConfiguration tiConf,
            TypeInformation<Event> resultTypeInfo,
            TiKVMetadataConverter[] metadataConverters,
            Map<String, RowType> physicalDataTypes,
            String sourceTimeZone,
            JdbcInfo jdbcInfo,
            List<TableId> tables) {

        super(tiConf, metadataConverters, physicalDataTypes, null);
        this.resultTypeInfo = checkNotNull(resultTypeInfo);
        this.sourceTimeZone = sourceTimeZone;
        this.jdbcInfo = jdbcInfo;
        this.tables = tables;
    }

    @Override
    public void deserialize(Kvrpcpb.KvPair record, Collector out) throws Exception {
        if (tableIdMap == null) {
            tableIdMap = fetchTableIdMap(tables);
        }
        long tableIdNum = RowKey.decode(record.getKey().toByteArray()).getTableId();
        TableInfo tableDto = tableIdMap.get(tableIdNum);
        TiTableInfo tableInfo = tableDto.getTableInfo();

        String database = tableDto.getTableName().split("\\.")[0];
        String tableName = tableDto.getTableName().split("\\.")[1];
        com.ververica.cdc.common.schema.Schema schema =
                SchemaUtils.getSchema(tableDto.getTableName(), jdbcInfo);
        Object[] tikvValues =
                decodeObjects(
                        record.getValue().toByteArray(),
                        RowKey.decode(record.getKey().toByteArray()).getHandle(),
                        tableInfo);
        TiKVDeserializationRuntimeConverter physicalConverter =
                physicalConverterMap.get(database + "." + tableName);
        GenericRowData row =
                (GenericRowData) physicalConverter.convert(tikvValues, tableInfo, null);
        RowConvertUtils.sourceTimeZone = sourceTimeZone;
        DataChangeEvent event =
                DataChangeEvent.insertEvent(
                        TableId.tableId(database, tableName),
                        RowConvertUtils.convert(row, schema),
                        new HashMap<>());
        out.collect(event);
    }

    @Override
    public TypeInformation getProducedType() {
        return resultTypeInfo;
    }
}
