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

package com.ververica.cdc.connectors.oracle.utils;

import org.apache.flink.table.data.GenericRowData;

import com.ververica.cdc.common.data.DecimalData;
import com.ververica.cdc.common.data.LocalZonedTimestampData;
import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.data.TimestampData;
import com.ververica.cdc.common.data.binary.BinaryStringData;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DecimalType;
import com.ververica.cdc.common.utils.Preconditions;
import com.ververica.cdc.debezium.table.DeserializationRuntimeConverter;
import com.ververica.cdc.debezium.utils.TemporalConversions;
import com.ververica.cdc.debezium.utils.TimeStampDataUtils;
import com.ververica.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.kafka.connect.data.Schema;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;

/** Utilities for converting from MySQL types to {@link DataType}s. */
public class RowConvertUtils {
    public static String sourceTimeZone;
    private static final ZoneId timeZoneUTC = ZoneId.of("UTC");
    /** Creates a runtime converter which assuming input object is not null. */
    protected static DeserializationRuntimeConverter createNotNullConverter(DataType type) {
        // if no matched user defined converter, fallback to the default converter
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return (Object dbzObj, Schema schema) -> convertToBoolean(dbzObj, schema);
            case TINYINT:
                return (Object dbzObj, Schema schema) -> convertToByte(dbzObj, schema);
            case SMALLINT:
                return (Object dbzObj, Schema schema) -> convertToShort(dbzObj, schema);
            case INTEGER:
                return (Object dbzObj, Schema schema) -> convertToInt(dbzObj, schema);
            case BIGINT:
                return (Object dbzObj, Schema schema) -> convertToLong(dbzObj, schema);
            case DATE:
                return (Object dbzObj, Schema schema) -> convertToDate(dbzObj, schema);
            case TIME_WITHOUT_TIME_ZONE:
                return (Object dbzObj, Schema schema) -> convertToTime(dbzObj, schema);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (Object dbzObj, Schema schema) -> convertToTimestamp(dbzObj, schema);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return (Object dbzObj, Schema schema) ->
                        convertToLocalTimeZoneTimestamp(dbzObj, schema);
            case FLOAT:
                return (Object dbzObj, Schema schema) -> convertToFloat(dbzObj, schema);
            case DOUBLE:
                return (Object dbzObj, Schema schema) -> convertToDouble(dbzObj, schema);
            case CHAR:
            case VARCHAR:
                return (Object dbzObj, Schema schema) -> convertToString(dbzObj, schema);
            case BINARY:
            case VARBINARY:
                return (Object dbzObj, Schema schema) -> convertToBinary(dbzObj, schema);
            case DECIMAL:
                return new DeserializationRuntimeConverter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(Object dbzObj, Schema schema) {
                        return convertToDecimal((DecimalType) type, dbzObj, schema);
                    }
                };
            case ARRAY:
            case MAP:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    protected static Object convertToBoolean(Object dbzObj, Schema schema) {
        if (dbzObj instanceof Boolean) {
            return dbzObj;
        } else if (dbzObj instanceof Byte) {
            return (byte) dbzObj == 1;
        } else if (dbzObj instanceof Short) {
            return (short) dbzObj == 1;
        } else {
            return Boolean.parseBoolean(dbzObj.toString());
        }
    }

    protected static Object convertToByte(Object dbzObj, Schema schema) {
        return Byte.parseByte(dbzObj.toString());
    }

    protected static Object convertToShort(Object dbzObj, Schema schema) {
        return Short.parseShort(dbzObj.toString());
    }

    protected static Object convertToInt(Object dbzObj, Schema schema) {
        if (dbzObj instanceof Integer) {
            return dbzObj;
        } else if (dbzObj instanceof Long) {
            return ((Long) dbzObj).intValue();
        } else {
            return Integer.parseInt(dbzObj.toString());
        }
    }

    protected static Object convertToLong(Object dbzObj, Schema schema) {
        if (dbzObj instanceof Integer) {
            return ((Integer) dbzObj).longValue();
        } else if (dbzObj instanceof Long) {
            return dbzObj;
        } else {
            return Long.parseLong(dbzObj.toString());
        }
    }

    protected static Object convertToDouble(Object dbzObj, Schema schema) {
        if (dbzObj instanceof Float) {
            return ((Float) dbzObj).doubleValue();
        } else if (dbzObj instanceof Double) {
            return dbzObj;
        } else {
            return Double.parseDouble(dbzObj.toString());
        }
    }

    protected static Object convertToFloat(Object dbzObj, Schema schema) {
        if (dbzObj instanceof Float) {
            return dbzObj;
        } else if (dbzObj instanceof Double) {
            return ((Double) dbzObj).floatValue();
        } else {
            return Float.parseFloat(dbzObj.toString());
        }
    }

    protected static Object convertToDate(Object dbzObj, Schema schema) {
        return (int) TemporalConversions.toLocalDate(dbzObj).toEpochDay();
    }

    protected static Object convertToTime(Object dbzObj, Schema schema) {
        if (dbzObj instanceof Long) {
            return (int) ((long) dbzObj / 1000);
        } else if (dbzObj instanceof Integer) {
            return dbzObj;
        }
        // get number of milliseconds of the day
        return TemporalConversions.toLocalTime(dbzObj).toSecondOfDay() * 1000;
    }

    protected static Object convertToTimestamp(Object dbzObj, Schema schema) {
        if (dbzObj instanceof Long) {
            TimestampData sourceTime = TimestampData.fromMillis((Long) dbzObj);
            return convertToStandardTimeStamp(sourceTime, sourceTimeZone);
        } else if (dbzObj instanceof org.apache.flink.table.data.TimestampData) {
            org.apache.flink.table.data.TimestampData timestampData =
                    (org.apache.flink.table.data.TimestampData) dbzObj;
            return convertToStandardTimeStamp(
                    TimestampData.fromMillis(timestampData.getMillisecond()), sourceTimeZone);
        }
        throw new IllegalArgumentException(
                "Unable to convert to TIMESTAMP from unexpected value '"
                        + dbzObj
                        + "' of type "
                        + dbzObj.getClass().getName());
    }

    public static TimestampData convertToStandardTimeStamp(
            TimestampData sourceTime, String sourceTimeZone) {
        if (timeZoneUTC.equals(ZoneId.of(sourceTimeZone))) {
            return sourceTime;
        }
        return TimeStampDataUtils.convertTimeZone(
                sourceTime, ZoneId.of(sourceTimeZone), timeZoneUTC);
    }

    protected static Object convertToLocalTimeZoneTimestamp(Object dbzObj, Schema schema) {
        if (dbzObj instanceof String) {
            String str = (String) dbzObj;
            // TIMESTAMP_LTZ type is encoded in string type
            Instant instant = Instant.parse(str);
            return LocalZonedTimestampData.fromInstant(instant);
        } else if (dbzObj instanceof org.apache.flink.table.data.TimestampData) {
            org.apache.flink.table.data.TimestampData timestampData =
                    (org.apache.flink.table.data.TimestampData) dbzObj;
            return LocalZonedTimestampData.fromInstant(timestampData.toInstant());
        }
        throw new IllegalArgumentException(
                "Unable to convert to TIMESTAMP_LTZ from unexpected value '"
                        + dbzObj
                        + "' of type "
                        + dbzObj.getClass().getName());
    }

    protected static Object convertToString(Object dbzObj, Schema schema) {
        if (dbzObj == null) {
            return null;
        } else {
            return BinaryStringData.fromString(dbzObj.toString());
        }
    }

    protected static Object convertToBinary(Object dbzObj, Schema schema) {
        if (dbzObj instanceof byte[]) {
            return dbzObj;
        } else if (dbzObj instanceof ByteBuffer) {
            ByteBuffer byteBuffer = (ByteBuffer) dbzObj;
            byte[] bytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(bytes);
            return bytes;
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported BYTES value type: " + dbzObj.getClass().getSimpleName());
        }
    }

    protected static Object convertToDecimal(
            DecimalType decimalType, Object dbzObj, Schema schema) {
        final int precision = decimalType.getPrecision();
        final int scale = decimalType.getScale();
        BigDecimal bigDecimal;
        if (dbzObj instanceof String) {
            // decimal.handling.mode=string
            bigDecimal = new BigDecimal((String) dbzObj);
        } else if (dbzObj instanceof Double) {
            // decimal.handling.mode=double
            bigDecimal = BigDecimal.valueOf((Double) dbzObj);
        } else {
            // fallback to string
            bigDecimal = new BigDecimal(dbzObj.toString());
        }
        return DecimalData.fromBigDecimal(bigDecimal, precision, scale);
    }

    public static RecordData convert(
            GenericRowData rowData, com.ververica.cdc.common.schema.Schema schema)
            throws Exception {
        int arity = rowData.getArity();

        DataType[] fields = schema.getColumnDataTypes().toArray(new DataType[0]);
        String[] columnNames = schema.getColumnNames().toArray(new String[0]);
        BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(fields);
        Object[] fieldsArr = new Object[arity];
        for (int i = 0; i < arity; i++) {
            DataType dataType = schema.getColumn(columnNames[i]).get().getType();
            if (rowData.getField(i) == null) {
                fieldsArr[i] = null;
            } else {
                Object convertedField =
                        createNotNullConverter(dataType).convert(rowData.getField(i), null);
                fieldsArr[i] = convertedField;
            }
        }
        Preconditions.checkState(
                fields.length == fieldsArr.length, "Column size does not match the data size");
        return generator.generate(fieldsArr);
    }
}
