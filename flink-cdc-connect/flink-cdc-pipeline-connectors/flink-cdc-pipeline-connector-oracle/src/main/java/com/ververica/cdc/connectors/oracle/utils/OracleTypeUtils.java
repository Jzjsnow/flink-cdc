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

import com.ververica.cdc.common.types.BigIntType;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.common.types.DecimalType;
import com.ververica.cdc.common.types.FloatType;
import com.ververica.cdc.common.types.LocalZonedTimestampType;
import com.ververica.cdc.common.types.TimestampType;
import com.ververica.cdc.common.types.VarCharType;
import com.ververica.cdc.common.types.ZonedTimestampType;
import io.debezium.relational.Column;

/** Utilities for converting from oracle types to {@link DataType}s. */
public class OracleTypeUtils {

    /** Returns a corresponding Flink data type from a debezium {@link Column}. */
    public static DataType fromDbzColumn(Column column) {
        DataType dataType = convertFromColumn(column);
        if (column.isOptional()) {
            return dataType;
        } else {
            return dataType.notNull();
        }
    }

    /**
     * Returns a corresponding Flink data type from a debezium {@link Column} with nullable always
     * be true.
     */
    private static DataType convertFromColumn(Column column) {
        String type = column.typeName();
        DataType dataType;
        switch (type.toUpperCase()) {
            case "VARCHAR2":
            case "NVARCHAR2":
            case "CHAR":
                dataType = new VarCharType(column.length());
                break;
            case "BLOB":
            case "CLOB":
            case "TEXT":
                dataType = DataTypes.STRING();
                break;
            case "NUMBER":
                dataType =
                        column.length() == 0
                                ? new DecimalType()
                                : new DecimalType(column.length(), column.scale().get());
                break;
            case "LONG":
                dataType = new BigIntType();
                break;
            case "DATE":
                dataType = new TimestampType(6);
                break;
            case "FLOAT":
                dataType = new FloatType();
                break;
            case "TIMESTAMP(1)":
            case "TIMESTAMP(3)":
            case "TIMESTAMP(6)":
            case "TIMESTAMP(9)":
                dataType = new TimestampType(6);
                break;
            case "TIMESTAMP(9) WITH TIME ZONE":
            case "TIMESTAMP(6) WITH TIME ZONE":
            case "TIMESTAMP(3) WITH TIME ZONE":
            case "TIMESTAMP(13) WITH TIME ZONE":
                dataType = new ZonedTimestampType();
                break;
            case "TIMESTAMP(6) WITH LOCAL TIME ZONE":
                dataType = new LocalZonedTimestampType();
                break;
            default:
                throw new RuntimeException("Unsupported data type:" + type);
        }
        return dataType;
    }

    private OracleTypeUtils() {}
}
