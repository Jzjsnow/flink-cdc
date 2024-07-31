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

import org.apache.flink.table.types.logical.LogicalTypeRoot;

import com.ververica.cdc.common.data.ArrayData;
import com.ververica.cdc.common.data.DecimalData;
import com.ververica.cdc.common.data.LocalZonedTimestampData;
import com.ververica.cdc.common.data.MapData;
import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.data.StringData;
import com.ververica.cdc.common.data.TimestampData;
import com.ververica.cdc.common.data.ZonedTimestampData;
import com.ververica.cdc.common.types.LocalZonedTimestampType;
import com.ververica.cdc.common.types.TimestampType;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;

import static com.ververica.cdc.connectors.iceberg.sink.IcebergRowConverter.createNullableExternalConverter;

/** A test for {@link IcebergRowConverter} . */
public class IcebergRowConverterTest {

    @Test
    public void testTimeStampDataConverter() {
        long timestamp = 1704038401000L; // = 2023-12-31T16:00:01Z = 2024-01-01T00:00:01+8:00
        ZoneId pipelineZoneId = ZoneId.of("Asia/Shanghai");
        LocalDateTime expectedLocalDateTime = LocalDateTime.of(2024, 1, 1, 0, 0, 1, 0);
        RecordData sourceRecordData = mockSourceRecordData(timestamp);

        testConvertFromTimeStampWithoutTimeZoneToTimeStampWithoutTimeZone(
                sourceRecordData, pipelineZoneId, expectedLocalDateTime);
        testConvertFromTimeStampWithoutTimeZoneToTimeStampWithTimeZone(
                sourceRecordData, pipelineZoneId, timestamp);
        testConvertFromTimeStampWithTimeZoneToTimeStampWithoutTimeZone(
                sourceRecordData, pipelineZoneId, expectedLocalDateTime);
        testConvertFromTimeStampWithTimeZoneToTimeStampWithTimeZone(
                sourceRecordData, pipelineZoneId, timestamp);
    }

    private void testConvertFromTimeStampWithoutTimeZoneToTimeStampWithoutTimeZone(
            RecordData sourceRecordData,
            ZoneId pipelineZoneId,
            LocalDateTime expectedLocalDateTime) {
        IcebergRowConverter.SerializationConverter converter =
                createNullableExternalConverter(
                        new TimestampType(true, 6),
                        pipelineZoneId,
                        LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE);
        org.apache.flink.table.data.TimestampData afterSerialization =
                (org.apache.flink.table.data.TimestampData)
                        converter.serialize(0, sourceRecordData);

        Assert.assertEquals(expectedLocalDateTime, afterSerialization.toLocalDateTime());
    }

    private void testConvertFromTimeStampWithoutTimeZoneToTimeStampWithTimeZone(
            RecordData sourceRecordData, ZoneId pipelineZoneId, long expectedTimestamp) {
        IcebergRowConverter.SerializationConverter converter =
                createNullableExternalConverter(
                        new TimestampType(true, 6),
                        pipelineZoneId,
                        LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
        org.apache.flink.table.data.TimestampData afterSerialization =
                (org.apache.flink.table.data.TimestampData)
                        converter.serialize(0, sourceRecordData);

        Assert.assertEquals(expectedTimestamp, afterSerialization.getMillisecond());
    }

    private void testConvertFromTimeStampWithTimeZoneToTimeStampWithoutTimeZone(
            RecordData sourceRecordData,
            ZoneId pipelineZoneId,
            LocalDateTime expectedLocalDateTime) {
        IcebergRowConverter.SerializationConverter converter =
                createNullableExternalConverter(
                        new LocalZonedTimestampType(true, 6),
                        pipelineZoneId,
                        LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE);
        org.apache.flink.table.data.TimestampData afterSerialization =
                (org.apache.flink.table.data.TimestampData)
                        converter.serialize(0, sourceRecordData);

        Assert.assertEquals(expectedLocalDateTime, afterSerialization.toLocalDateTime());
    }

    private void testConvertFromTimeStampWithTimeZoneToTimeStampWithTimeZone(
            RecordData sourceRecordData, ZoneId pipelineZoneId, long expectedTimestamp) {
        IcebergRowConverter.SerializationConverter converter =
                createNullableExternalConverter(
                        new LocalZonedTimestampType(true, 6),
                        pipelineZoneId,
                        LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
        org.apache.flink.table.data.TimestampData afterSerialization =
                (org.apache.flink.table.data.TimestampData)
                        converter.serialize(0, sourceRecordData);

        Assert.assertEquals(expectedTimestamp, afterSerialization.getMillisecond());
    }

    private RecordData mockSourceRecordData(long timestamp) {
        TimestampData sourceTimestampData = TimestampData.fromMillis(timestamp);
        LocalZonedTimestampData sourceLocalZonedTimestampData =
                LocalZonedTimestampData.fromEpochMillis(timestamp);
        return new RecordData() {
            @Override
            public int getArity() {
                return 1;
            }

            @Override
            public boolean isNullAt(int pos) {
                return false;
            }

            @Override
            public boolean getBoolean(int pos) {
                return false;
            }

            @Override
            public byte getByte(int pos) {
                return 0;
            }

            @Override
            public short getShort(int pos) {
                return 0;
            }

            @Override
            public int getInt(int pos) {
                return 0;
            }

            @Override
            public long getLong(int pos) {
                return 0;
            }

            @Override
            public float getFloat(int pos) {
                return 0;
            }

            @Override
            public double getDouble(int pos) {
                return 0;
            }

            @Override
            public byte[] getBinary(int pos) {
                return new byte[0];
            }

            @Override
            public StringData getString(int pos) {
                return null;
            }

            @Override
            public DecimalData getDecimal(int pos, int precision, int scale) {
                return null;
            }

            @Override
            public TimestampData getTimestamp(int pos, int precision) {
                return sourceTimestampData;
            }

            @Override
            public ZonedTimestampData getZonedTimestamp(int pos, int precision) {
                return null;
            }

            @Override
            public LocalZonedTimestampData getLocalZonedTimestampData(int pos, int precision) {
                return sourceLocalZonedTimestampData;
            }

            @Override
            public ArrayData getArray(int pos) {
                return null;
            }

            @Override
            public MapData getMap(int pos) {
                return null;
            }

            @Override
            public RecordData getRow(int pos, int numFields) {
                return null;
            }
        };
    }
}
