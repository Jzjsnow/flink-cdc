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

import io.debezium.relational.TableId;

/** Utils to prepare SQL statement. */
public class StatementUtils {

    private StatementUtils() {}

    public static String quote(String dbOrTableName) {
        return "`" + dbOrTableName + "`";
    }

    public static String quote(TableId tableId) {
        return tableId.toQuotedString('`');
    }
}
