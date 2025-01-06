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
import com.ververica.cdc.connectors.tidb.table.TiKVMetadataConverter;
import com.ververica.cdc.debezium.table.DeserializationRuntimeConverter;
import org.tikv.common.TiConfiguration;
import org.tikv.common.key.RowKey;
import org.tikv.kvproto.Kvrpcpb;

import java.util.HashMap;
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

    private String database;
    private String tableName;

    private final String sourceTimeZone;

    private JdbcInfo jdbcInfo;

    public TidbSnapshotEventDeserializer(
            TiConfiguration tiConf,
            String database,
            String tableName,
            TypeInformation<Event> resultTypeInfo,
            TiKVMetadataConverter[] metadataConverters,
            RowType physicalDataType,
            String sourceTimeZone,
            JdbcInfo jdbcInfo) {

        super(tiConf, database, tableName, metadataConverters, physicalDataType);
        this.database = database;
        this.tableName = tableName;
        this.resultTypeInfo = checkNotNull(resultTypeInfo);
        this.sourceTimeZone = sourceTimeZone;
        this.jdbcInfo = jdbcInfo;
    }

    @Override
    public void deserialize(Kvrpcpb.KvPair record, Collector out) throws Exception {
        if (tableInfo == null) {
            tableInfo = fetchTableInfo();
        }
        TableId tableId = TableId.tableId(database, tableName);
        io.debezium.relational.TableId relationalTableId =
                io.debezium.relational.TableId.parse(database + "." + tableName);
        com.ververica.cdc.common.schema.Schema schema =
                SchemaUtils.getSchema(relationalTableId, jdbcInfo);

        Object[] tikvValues =
                decodeObjects(
                        record.getValue().toByteArray(),
                        RowKey.decode(record.getKey().toByteArray()).getHandle(),
                        tableInfo);
        GenericRowData row =
                (GenericRowData) physicalConverter.convert(tikvValues, tableInfo, null);
        RowConvertUtils.sourceTimeZone = sourceTimeZone;
        DataChangeEvent event =
                DataChangeEvent.insertEvent(
                        tableId, RowConvertUtils.convert(row, schema), new HashMap<>());
        out.collect(event);
    }

    @Override
    public TypeInformation getProducedType() {
        return resultTypeInfo;
    }
}
