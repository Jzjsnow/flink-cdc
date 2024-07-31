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

package com.ververica.cdc.debezium.utils;

import com.ververica.cdc.common.data.TimestampData;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/** Supports {@link TimeStampDataUtils} to schema evolution. */
public class TimeStampDataUtils {
    /**
     * Convert {@link TimestampData} in the input time zone to the corresponding {@link
     * TimestampData} in the output time zone.
     *
     * <p>e.g. input an instance of {@link TimestampData} containing a millisecond of 1704038401000L
     * which represents 2023-12-31 16:00:01. Assuming that the time is in the time zone
     * inputZone=UTC, in order to get the time in the outputZone=UTC+8 time zone at that moment, a
     * {@link TimestampData} representing 2024-01-01 00:00:01 is expected, which should contain a
     * milliseconds of 1704067201000L.
     */
    public static TimestampData convertTimeZone(
            TimestampData timestampData, ZoneId inputZone, ZoneId outputZone) {
        ZonedDateTime inputDateTime = ZonedDateTime.of(timestampData.toLocalDateTime(), inputZone);
        // Switches the time in the input time zone to the time in the output time zone
        ZonedDateTime outputDateTime = inputDateTime.withZoneSameInstant(outputZone);
        LocalDateTime localDateTime = outputDateTime.toLocalDateTime();
        return TimestampData.fromLocalDateTime(localDateTime);
    }
}
