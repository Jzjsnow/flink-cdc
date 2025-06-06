/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.doris.sink;

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.exception.IllegalArgumentException;
import org.apache.doris.flink.sink.schema.SchemaChangeManager;

import java.io.IOException;

import static org.apache.doris.flink.catalog.doris.DorisSchemaFactory.identifier;

/** An enriched version of Doris' {@link SchemaChangeManager}. */
public class DorisSchemaChangeManager extends SchemaChangeManager {
    public DorisSchemaChangeManager(DorisOptions dorisOptions, String charsetEncoding) {
        super(dorisOptions, charsetEncoding);
    }

    public boolean truncateTable(String databaseName, String tableName)
            throws IOException, IllegalArgumentException {
        String truncateTableDDL =
                "TRUNCATE TABLE " + identifier(databaseName) + "." + identifier(tableName);
        return this.execute(truncateTableDDL, databaseName);
    }

    public boolean dropTable(String databaseName, String tableName)
            throws IOException, IllegalArgumentException {
        String dropTableDDL =
                "DROP TABLE " + identifier(databaseName) + "." + identifier(tableName);
        return this.execute(dropTableDDL, databaseName);
    }
}
