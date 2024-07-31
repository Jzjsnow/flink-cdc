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

package com.ververica.cdc.connectors.iceberg.utils;

import org.apache.flink.table.data.TimestampData;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/** Supports {@link TimeStampDataUtils} to schema evolution. */
public class TimeStampDataUtils {
    /**
     * Returns {@link org.apache.flink.table.data.TimestampData} in the specified time zone. For an
     * input standard timestamp (Instant), provides the display time in the specified time zone.
     *
     * <p>e.g. input instant = 1704038401000L (=2023-12-31T16:00:01Z) and outputZone = UTC+8, the
     * returned {@link org.apache.flink.table.data.TimestampData} corresponds to 2024-01-01
     * 00:00:01; if outputZone = UTC+10, the returned {@link
     * org.apache.flink.table.data.TimestampData} corresponds to 2024-01-01 02:00:01
     */
    public static TimestampData convertTimeZone(Instant instant, ZoneId outputZone) {
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(instant, outputZone);
        LocalDateTime localDateTime = zonedDateTime.toLocalDateTime();
        return TimestampData.fromLocalDateTime(localDateTime);
    }
}
