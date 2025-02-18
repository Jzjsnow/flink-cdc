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

package com.ververica.cdc.connectors.tidb.table.dto;

import org.tikv.common.meta.TiTableInfo;

import java.io.Serializable;

/** used to generate field definition in jdbc connect. */
public class TableInfo implements Serializable {
    private long tableId;
    private String tableName;
    private TiTableInfo tableInfo;

    public long getTableId() {
        return tableId;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public TiTableInfo getTableInfo() {
        return tableInfo;
    }

    public void setTableInfo(TiTableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }
}
