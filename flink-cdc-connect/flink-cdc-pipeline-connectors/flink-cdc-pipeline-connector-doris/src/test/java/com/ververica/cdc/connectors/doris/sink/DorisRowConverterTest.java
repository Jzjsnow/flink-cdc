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

package com.ververica.cdc.connectors.doris.sink;

import com.ververica.cdc.common.data.ArrayData;
import com.ververica.cdc.common.data.DecimalData;
import com.ververica.cdc.common.data.LocalZonedTimestampData;
import com.ververica.cdc.common.data.MapData;
import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.data.StringData;
import com.ververica.cdc.common.data.TimestampData;
import com.ververica.cdc.common.data.ZonedTimestampData;
import com.ververica.cdc.common.data.binary.BinaryRecordData;
import com.ververica.cdc.common.data.binary.BinaryStringData;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.common.types.LocalZonedTimestampType;
import com.ververica.cdc.common.types.RowType;
import com.ververica.cdc.common.types.TimestampType;
import com.ververica.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** A test for {@link DorisRowConverter} . */
public class DorisRowConverterTest {

    @Test
    public void testExternalConvert() {
        List<Column> columns =
                Arrays.asList(
                        Column.physicalColumn("f2", DataTypes.BOOLEAN()),
                        Column.physicalColumn("f3", DataTypes.FLOAT()),
                        Column.physicalColumn("f4", DataTypes.DOUBLE()),
                        Column.physicalColumn("f7", DataTypes.TINYINT()),
                        Column.physicalColumn("f8", DataTypes.SMALLINT()),
                        Column.physicalColumn("f9", DataTypes.INT()),
                        Column.physicalColumn("f10", DataTypes.BIGINT()),
                        Column.physicalColumn("f12", DataTypes.TIMESTAMP()),
                        Column.physicalColumn("f14", DataTypes.DATE()),
                        Column.physicalColumn("f15", DataTypes.CHAR(1)),
                        Column.physicalColumn("f16", DataTypes.VARCHAR(256)),
                        Column.physicalColumn("f17", DataTypes.TIMESTAMP()),
                        Column.physicalColumn("f18", DataTypes.TIMESTAMP()),
                        Column.physicalColumn("f19", DataTypes.TIMESTAMP()),
                        Column.physicalColumn("f20", DataTypes.TIMESTAMP_LTZ()),
                        Column.physicalColumn("f21", DataTypes.TIMESTAMP_LTZ()),
                        Column.physicalColumn("f22", DataTypes.TIMESTAMP_LTZ()));

        List<DataType> dataTypes =
                columns.stream().map(v -> v.getType()).collect(Collectors.toList());
        LocalDateTime time1 =
                LocalDateTime.ofInstant(Instant.parse("2021-01-01T08:00:00Z"), ZoneId.of("Z"));
        LocalDate date1 = LocalDate.of(2021, 1, 1);

        LocalDateTime f17 =
                LocalDateTime.ofInstant(Instant.parse("2021-01-01T08:01:11Z"), ZoneId.of("Z"));
        LocalDateTime f18 =
                LocalDateTime.ofInstant(Instant.parse("2021-01-01T08:01:11.123Z"), ZoneId.of("Z"));
        LocalDateTime f19 =
                LocalDateTime.ofInstant(
                        Instant.parse("2021-01-01T08:01:11.123456Z"), ZoneId.of("Z"));

        Instant f20 = Instant.parse("2021-01-01T08:01:11Z");
        Instant f21 = Instant.parse("2021-01-01T08:01:11.123Z");
        Instant f22 = Instant.parse("2021-01-01T08:01:11.123456Z");

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(RowType.of(dataTypes.toArray(new DataType[] {})));
        BinaryRecordData recordData =
                generator.generate(
                        new Object[] {
                            true,
                            1.2F,
                            1.2345D,
                            (byte) 1,
                            (short) 32,
                            64,
                            128L,
                            TimestampData.fromLocalDateTime(time1),
                            (int) date1.toEpochDay(),
                            BinaryStringData.fromString("a"),
                            BinaryStringData.fromString("doris"),
                            TimestampData.fromLocalDateTime(f17),
                            TimestampData.fromLocalDateTime(f18),
                            TimestampData.fromLocalDateTime(f19),
                            LocalZonedTimestampData.fromInstant(f20),
                            LocalZonedTimestampData.fromInstant(f21),
                            LocalZonedTimestampData.fromInstant(f22),
                        });
        List row = new ArrayList();
        for (int i = 0; i < recordData.getArity(); i++) {
            DorisRowConverter.SerializationConverter converter =
                    DorisRowConverter.createNullableExternalConverter(
                            columns.get(i).getType(), ZoneId.of("GMT+08:00"));
            row.add(converter.serialize(i, recordData));
        }
        Assert.assertEquals(
                "[true, 1.2, 1.2345, 1, 32, 64, 128, 2021-01-01 08:00:00.000000, 2021-01-01, a, doris, 2021-01-01 "
                        + "08:01:11.000000, 2021-01-01 08:01:11.123000, 2021-01-01 08:01:11.123456, 2021-01-01 "
                        + "16:01:11.000000, 2021-01-01 16:01:11.123000, 2021-01-01 16:01:11.123456]",
                row.toString());
    }

    @Test
    public void testConvertTimeStampWithoutTimeZone() {
        long timestamp = 1704038401000L; // = 2023-12-31T16:00:01Z = 2024-01-01T00:00:01+8:00
        ZoneId zoneId = ZoneId.of("Asia/Shanghai");
        String expectedLocalTimeString = "2024-01-01 00:00:01.000000";

        RecordData sourceRecordData = mockSourceRecordData(timestamp);
        DorisRowConverter.SerializationConverter converter =
                DorisRowConverter.createNullableExternalConverter(
                        new TimestampType(true, 6), zoneId);
        Object field = converter.serialize(0, sourceRecordData);

        Assert.assertEquals(expectedLocalTimeString, field);
    }

    @Test
    public void testConvertTimeStampWithTimeZone() {
        long timestamp = 1704038401000L; // = 2023-12-31T16:00:01Z = 2024-01-01T00:00:01+8:00
        ZoneId zoneId = ZoneId.of("Asia/Shanghai");
        String expectedLocalTimeString = "2024-01-01 00:00:01.000000";

        RecordData sourceRecordData = mockSourceRecordData(timestamp);
        DorisRowConverter.SerializationConverter converter =
                DorisRowConverter.createNullableExternalConverter(
                        new LocalZonedTimestampType(true, 6), zoneId);
        Object field = converter.serialize(0, sourceRecordData);

        Assert.assertEquals(expectedLocalTimeString, field);
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
