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

package com.ververica.cdc.connectors.mysql.source;

import com.ververica.cdc.common.data.TimestampData;
import com.ververica.cdc.common.source.SupportedMetadataColumn;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DataTypes;

import java.util.Map;

/** A {@link SupportedMetadataColumn} for op_ts. */
public class OpTsMetadataColumn implements SupportedMetadataColumn {
    @Override
    public String getName() {
        return "op_ts";
    }

    @Override
    public DataType getType() {
        return DataTypes.TIMESTAMP().nullable();
    }

    @Override
    public Class<?> getJavaClass() {
        return TimestampData.class;
    }

    @Override
    public Object read(Map<String, String> metadata) {
        if (metadata.containsKey(getName())) {
            // The default value of op_ts for snapshot data is 0. If op_ts=0, a NULL value is
            // returned rather than converted to 1970-01-01T00:00:00
            long timestamp = Long.parseLong(metadata.get(getName()));
            return timestamp > 0 ? TimestampData.fromMillis(timestamp) : null;
        }
        throw new IllegalArgumentException("op_ts doesn't exist in the metadata: " + metadata);
    }
}
