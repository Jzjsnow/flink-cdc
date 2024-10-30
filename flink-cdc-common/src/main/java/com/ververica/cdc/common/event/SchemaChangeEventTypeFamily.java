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

package com.ververica.cdc.common.event;

import com.ververica.cdc.common.annotation.PublicEvolving;

import static com.ververica.cdc.common.event.SchemaChangeEventType.ADD_COLUMN;
import static com.ververica.cdc.common.event.SchemaChangeEventType.ALTER_COLUMN_TYPE;
import static com.ververica.cdc.common.event.SchemaChangeEventType.CREATE_TABLE;
import static com.ververica.cdc.common.event.SchemaChangeEventType.DROP_COLUMN;
import static com.ververica.cdc.common.event.SchemaChangeEventType.DROP_TABLE;
import static com.ververica.cdc.common.event.SchemaChangeEventType.RENAME_COLUMN;
import static com.ververica.cdc.common.event.SchemaChangeEventType.TRUNCATE_TABLE;

/**
 * An enumeration of schema change event families for clustering {@link SchemaChangeEvent}s into
 * categories.
 */
@PublicEvolving
public class SchemaChangeEventTypeFamily {

    public static final SchemaChangeEventType[] ADD = {ADD_COLUMN};

    public static final SchemaChangeEventType[] ALTER = {ALTER_COLUMN_TYPE};

    public static final SchemaChangeEventType[] CREATE = {CREATE_TABLE};

    public static final SchemaChangeEventType[] DROP = {DROP_COLUMN, DROP_TABLE};

    public static final SchemaChangeEventType[] RENAME = {RENAME_COLUMN};

    public static final SchemaChangeEventType[] TABLE = {CREATE_TABLE, DROP_TABLE, TRUNCATE_TABLE};

    public static final SchemaChangeEventType[] COLUMN = {
        ADD_COLUMN, ALTER_COLUMN_TYPE, DROP_COLUMN, RENAME_COLUMN
    };

    public static final SchemaChangeEventType[] ALL = {
        ADD_COLUMN,
        ALTER_COLUMN_TYPE,
        CREATE_TABLE,
        DROP_COLUMN,
        DROP_TABLE,
        RENAME_COLUMN,
        TRUNCATE_TABLE
    };

    public static final SchemaChangeEventType[] NONE = {};

    public static SchemaChangeEventType[] ofTag(String tag) {
        switch (tag) {
            case "add":
                return ADD;
            case "alter":
                return ALTER;
            case "create":
                return CREATE;
            case "drop":
                return DROP;
            case "rename":
                return RENAME;
            case "table":
                return TABLE;
            case "column":
                return COLUMN;
            case "all":
                return ALL;
            default:
                return NONE;
        }
    }
}
