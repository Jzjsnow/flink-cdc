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

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.ververica.cdc.common.annotation.VisibleForTesting;
import com.ververica.cdc.common.data.ArrayData;
import com.ververica.cdc.common.data.GenericArrayData;
import com.ververica.cdc.common.data.GenericMapData;
import com.ververica.cdc.common.data.MapData;
import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.types.DataField;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DataTypeChecks;
import com.ververica.cdc.common.types.DecimalType;
import com.ververica.cdc.common.types.RowType;
import com.ververica.cdc.common.types.ZonedTimestampType;
import com.ververica.cdc.connectors.iceberg.utils.TimeStampDataUtils;

import java.io.IOException;
import java.io.Serializable;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** converter {@link RecordData} type object to iceberg field. */
public class IcebergRowConverter implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /** Runtime converter to convert {@link RecordData} type object to iceberg field. */
    @FunctionalInterface
    interface SerializationConverter extends Serializable {
        Object serialize(int index, RecordData field);
    }

    static SerializationConverter createNullableExternalConverter(
            DataType type, ZoneId pipelineZoneId) {
        return wrapIntoNullableExternalConverter(createExternalConverter(type, pipelineZoneId));
    }

    static SerializationConverter createNullableExternalConverter(
            DataType type, ZoneId pipelineZoneId, LogicalTypeRoot sinkDataType) {
        return wrapIntoNullableExternalConverter(
                createExternalConverter(type, pipelineZoneId, sinkDataType));
    }

    static SerializationConverter wrapIntoNullableExternalConverter(
            SerializationConverter serializationConverter) {
        return (index, val) -> {
            if (val == null || val.isNullAt(index)) {
                return null;
            } else {
                return serializationConverter.serialize(index, val);
            }
        };
    }

    static SerializationConverter createExternalConverter(DataType type, ZoneId pipelineZoneId) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return (index, val) -> val.getString(index).toString();
            case BOOLEAN:
                return (index, val) -> val.getBoolean(index);
            case BINARY:
            case VARBINARY:
                return (index, val) -> val.getBinary(index);
            case DECIMAL:
                final int decimalPrecision = ((DecimalType) type).getPrecision();
                final int decimalScale = ((DecimalType) type).getScale();
                return (index, val) ->
                        DecimalData.fromBigDecimal(
                                val.getDecimal(index, decimalPrecision, decimalScale)
                                        .toBigDecimal(),
                                decimalPrecision,
                                decimalScale);
            case TINYINT:
                return (index, val) -> val.getInt(index);
            case SMALLINT:
                return (index, val) -> val.getInt(index);
            case INTEGER:
                return (index, val) -> val.getInt(index);
            case BIGINT:
                return (index, val) -> val.getLong(index);
            case FLOAT:
                return (index, val) -> val.getFloat(index);
            case DOUBLE:
                return (index, val) -> val.getDouble(index);
            case DATE:
                return (index, val) -> val.getInt(index);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (index, val) ->
                        TimestampData.fromTimestamp(
                                val.getTimestamp(index, DataTypeChecks.getPrecision(type))
                                        .toTimestamp());

            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return (index, val) ->
                        TimestampData.fromInstant(
                                val.getLocalZonedTimestampData(
                                                index, DataTypeChecks.getPrecision(type))
                                        .toInstant());

            case TIMESTAMP_WITH_TIME_ZONE:
                final int zonedP = ((ZonedTimestampType) type).getPrecision();
                return (index, val) ->
                        TimestampData.fromTimestamp(val.getTimestamp(index, zonedP).toTimestamp());
            case ARRAY:
                return (index, val) -> convertArrayData(val.getArray(index), type);
            case MAP:
                return (index, val) -> writeValueAsString(convertMapData(val.getMap(index), type));
            case ROW:
                return (index, val) ->
                        writeValueAsString(convertRowData(val, index, type, pipelineZoneId));
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    static SerializationConverter createExternalConverter(
            DataType type, ZoneId pipelineZoneId, LogicalTypeRoot sinkDataType) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return (index, val) -> val.getString(index).toString();
            case BOOLEAN:
                return (index, val) -> val.getBoolean(index);
            case BINARY:
            case VARBINARY:
                return (index, val) -> val.getBinary(index);
            case DECIMAL:
                final int decimalPrecision = ((DecimalType) type).getPrecision();
                final int decimalScale = ((DecimalType) type).getScale();
                return (index, val) ->
                        DecimalData.fromBigDecimal(
                                val.getDecimal(index, decimalPrecision, decimalScale)
                                        .toBigDecimal(),
                                decimalPrecision,
                                decimalScale);
            case TINYINT:
                return (index, val) -> val.getInt(index);
            case SMALLINT:
                return (index, val) -> val.getInt(index);
            case INTEGER:
                return (index, val) -> val.getInt(index);
            case BIGINT:
                return (index, val) -> val.getLong(index);
            case FLOAT:
                return (index, val) -> val.getFloat(index);
            case DOUBLE:
                return (index, val) -> val.getDouble(index);
            case DATE:
                return (index, val) -> val.getInt(index);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return createTimeStampWithoutTimeZoneConverter(type, sinkDataType, pipelineZoneId);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return createTimeStampWithLocalTimeZoneConverter(
                        type, sinkDataType, pipelineZoneId);
            case TIMESTAMP_WITH_TIME_ZONE:
                final int zonedP = ((ZonedTimestampType) type).getPrecision();
                return createTimeStampWithTimeZoneConverter(zonedP, sinkDataType, pipelineZoneId);
            case ARRAY:
                return (index, val) -> convertArrayData(val.getArray(index), type);
            case MAP:
                return (index, val) -> writeValueAsString(convertMapData(val.getMap(index), type));
            case ROW:
                return (index, val) ->
                        writeValueAsString(convertRowData(val, index, type, pipelineZoneId));
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    /**
     * Since the {@link com.ververica.cdc.common.data.TimestampData} passed from source contains
     * epoch milliseconds, when writing to iceberg's TIMESTAMP_WITHOUT_TIME_ZONE field, it will be
     * displayed as UTC time. To display the time in a specified timezone, it needs to be converted
     * to the {@link org.apache.flink.table.data.TimestampData} for the corresponding time. e.g. A
     * source {@link com.ververica.cdc.common.data.TimestampData} instance contains a millisecond of
     * 1704038401000L, representing 2023-12-31T16:00:01Z. When writing to iceberg's
     * TIMESTAMP_WITHOUT_TIME_ZONE field, it will be displayed as 2023-12-31 16:00:01; if expecting
     * to display the time in the UTC+8 time zone, a {@link
     * org.apache.flink.table.data.TimestampData} representing 2024-01-01 00:00:01 (i.e. containing
     * a millisecond of 1704067201000 milliseconds) to sink is required. This conversion is
     * performed in the method {@link
     * com.ververica.cdc.connectors.iceberg.utils.TimeStampDataUtils}.convertTimeZone.
     */
    @VisibleForTesting
    public static SerializationConverter createTimeStampWithoutTimeZoneConverter(
            DataType sourceDataType, LogicalTypeRoot sinkDataType, ZoneId sinkZoneId) {
        return (index, val) -> {
            if (Objects.requireNonNull(sinkDataType)
                    == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) {
                return TimeStampDataUtils.convertTimeZone(
                        val.getTimestamp(index, DataTypeChecks.getPrecision(sourceDataType))
                                .toInstant(),
                        sinkZoneId);
            }
            return TimestampData.fromInstant(
                    val.getTimestamp(index, DataTypeChecks.getPrecision(sourceDataType))
                            .toInstant());
        };
    }

    /**
     * Since the {@link com.ververica.cdc.common.data.LocalZonedTimestampData} passed from source
     * contains epoch milliseconds, when writing to iceberg's TIMESTAMP_WITHOUT_TIME_ZONE field, it
     * will be displayed as UTC time. To display the time in a specified timezone, it needs to be
     * converted to the {@link org.apache.flink.table.data.TimestampData} for the corresponding
     * time. e.g. A source {@link com.ververica.cdc.common.data.LocalZonedTimestampData} instance
     * contains a millisecond of 1704038401000L, representing 2023-12-31T16:00:01Z. When writing to
     * iceberg's TIMESTAMP_WITHOUT_TIME_ZONE field, it will be displayed as 2023-12-31 16:00:01; if
     * expecting to display the time in the UTC+8 time zone, a {@link
     * org.apache.flink.table.data.TimestampData} representing 2024-01-01 00:00:01 (i.e. containing
     * a millisecond of 1704067201000 milliseconds) to sink is required. This conversion is
     * performed in the method {@link
     * com.ververica.cdc.connectors.iceberg.utils.TimeStampDataUtils}.convertTimeZone.
     */
    @VisibleForTesting
    public static SerializationConverter createTimeStampWithLocalTimeZoneConverter(
            DataType sourceDataType, LogicalTypeRoot sinkDataType, ZoneId sinkZoneId) {
        return (index, val) -> {
            if (Objects.requireNonNull(sinkDataType)
                    == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) {
                return TimeStampDataUtils.convertTimeZone(
                        val.getLocalZonedTimestampData(
                                        index, DataTypeChecks.getPrecision(sourceDataType))
                                .toInstant(),
                        sinkZoneId);
            }
            return TimestampData.fromInstant(
                    val.getLocalZonedTimestampData(
                                    index, DataTypeChecks.getPrecision(sourceDataType))
                            .toInstant());
        };
    }

    @VisibleForTesting
    public static SerializationConverter createTimeStampWithTimeZoneConverter(
            int precision, LogicalTypeRoot sinkDataType, ZoneId sinkZoneId) {
        return (index, val) -> {
            if (Objects.requireNonNull(sinkDataType)
                    == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) {
                return TimeStampDataUtils.convertTimeZone(
                        val.getTimestamp(index, precision).toInstant(), sinkZoneId);
            }
            return TimestampData.fromTimestamp(val.getTimestamp(index, precision).toTimestamp());
        };
    }

    private static List<Object> convertArrayData(ArrayData array, DataType type) {
        if (array instanceof GenericArrayData) {
            return Arrays.asList(((GenericArrayData) array).toObjectArray());
        }
        throw new UnsupportedOperationException("Unsupported array data: " + array.getClass());
    }

    private static Object convertMapData(MapData map, DataType type) {
        Map<Object, Object> result = new HashMap<>();
        if (map instanceof GenericMapData) {
            GenericMapData gMap = (GenericMapData) map;
            for (Object key : ((GenericArrayData) gMap.keyArray()).toObjectArray()) {
                result.put(key, gMap.get(key));
            }
            return result;
        }
        throw new UnsupportedOperationException("Unsupported map data: " + map.getClass());
    }

    private static Object convertRowData(
            RecordData val, int index, DataType type, ZoneId pipelineZoneId) {
        RowType rowType = (RowType) type;
        Map<String, Object> value = new HashMap<>();
        RecordData row = val.getRow(index, rowType.getFieldCount());

        List<DataField> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            DataField rowField = fields.get(i);
            SerializationConverter converter =
                    createNullableExternalConverter(rowField.getType(), pipelineZoneId);
            Object valTmp = converter.serialize(i, row);
            value.put(rowField.getName(), valTmp.toString());
        }
        return value;
    }

    private static String writeValueAsString(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
