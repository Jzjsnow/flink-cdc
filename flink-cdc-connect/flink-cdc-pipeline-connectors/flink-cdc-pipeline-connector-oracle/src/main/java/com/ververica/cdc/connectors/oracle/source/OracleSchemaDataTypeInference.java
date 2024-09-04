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

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.debezium.event.DebeziumSchemaDataTypeInference;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;
import org.apache.kafka.connect.data.Schema;

/** {@link DataType} inference for oracle debezium {@link Schema}. */
@Internal
public class OracleSchemaDataTypeInference extends DebeziumSchemaDataTypeInference {

    private static final long serialVersionUID = 1L;

    protected DataType inferStruct(Object value, Schema schema) {
        // the Geometry datatype in oracle will be converted to
        // a String with Json format
        if (Point.LOGICAL_NAME.equals(schema.name())
                || Geometry.LOGICAL_NAME.equals(schema.name())) {
            return DataTypes.STRING();
        } else {
            return super.inferStruct(value, schema);
        }
    }
}
