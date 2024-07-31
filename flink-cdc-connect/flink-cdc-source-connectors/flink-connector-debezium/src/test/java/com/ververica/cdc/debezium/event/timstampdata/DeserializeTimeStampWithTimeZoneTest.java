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

package com.ververica.cdc.debezium.event.timstampdata;

import com.ververica.cdc.common.data.TimestampData;
import com.ververica.cdc.debezium.utils.TimeStampDataUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;
import java.time.ZoneId;

import static com.ververica.cdc.debezium.event.DebeziumEventDeserializationSchema.convertToStandardTimeStamp;

/** Test for {@link TimestampData}. */
public class DeserializeTimeStampWithTimeZoneTest {
    /**
     * Test {@link TimeStampDataUtils#convertTimeZone} using {@link TimestampData} created by {@link
     * Timestamp}.
     */
    @Test
    public void testConvertTimeZoneUsingTimestampDataCreatedByTimestamp() {
        long timestamp = 1704038401000L; // = 2023-12-31T16:00:01Z = 2024-01-01T00:00:01+8:00
        ZoneId zoneId = ZoneId.of("Asia/Shanghai");
        Timestamp ts = Timestamp.valueOf("2024-01-01 00:00:01");

        TimestampData afterDeserialization =
                TimeStampDataUtils.convertTimeZone(
                        TimestampData.fromTimestamp(ts), zoneId, ZoneId.of("UTC"));

        Assert.assertEquals(timestamp, afterDeserialization.getMillisecond());
    }

    /**
     * Test {@link TimeStampDataUtils#convertTimeZone} using {@link TimestampData} created by
     * milliseconds.
     */
    @Test
    public void testConvertTimeZoneUsingTimestampDataCreatedByMillis() {
        ZoneId zoneId = ZoneId.of("Asia/Shanghai");
        long millisecond = 1704067201000L; // = 2024-01-01T00:00:01Z = 2024-01-01T08:00:01+8:00
        long timestamp = 1704038401000L; // = 2023-12-31T16:00:01Z = 2024-01-01T00:00:01+8:00

        TimestampData afterDeserialization =
                TimeStampDataUtils.convertTimeZone(
                        TimestampData.fromMillis(millisecond), zoneId, ZoneId.of("UTC"));

        Assert.assertEquals(timestamp, afterDeserialization.getMillisecond());
    }

    /**
     * Test {@link
     * com.ververica.cdc.debezium.event.DebeziumEventDeserializationSchema#convertToStandardTimeStamp}.
     */
    @Test
    public void testConvertToStandardTimeStamp() {
        Object dbzObj = 1704067201000L; // = 2024-01-01 00:00:01
        String sourceTimeZone = "Asia/Shanghai";
        TimestampData sourceTime = TimestampData.fromMillis((Long) dbzObj);
        TimestampData expectedStandardTime =
                TimestampData.fromMillis(1704038401000L); // = 2023-12-31 16:00:01

        TimestampData afterDeserialization = convertToStandardTimeStamp(sourceTime, sourceTimeZone);
        Assert.assertEquals(expectedStandardTime, afterDeserialization);
    }
}
