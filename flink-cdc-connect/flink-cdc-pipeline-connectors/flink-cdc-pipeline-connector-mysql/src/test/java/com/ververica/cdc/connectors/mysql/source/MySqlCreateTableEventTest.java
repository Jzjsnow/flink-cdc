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

import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import com.ververica.cdc.connectors.mysql.source.reader.MySqlPipelineRecordEmitter;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import io.debezium.relational.TableId;
import org.junit.Test;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link MySqlPipelineRecordEmitter}. */
public class MySqlCreateTableEventTest {

    @Test
    public void testAddUdalShardkeyColumns() {

        String createTableDdl =
                " CREATE TABLE test.tablename (\n"
                        + "   `column1` varchar(60) NOT NULL,\n"
                        + "   `column2` varchar(60) DEFAULT NULL,\n"
                        + "   `column3` varchar(60) DEFAULT NULL,\n"
                        + "   `acct_id` varchar(60) DEFAULT NULL,\n"
                        + "   PRIMARY KEY (`column1`)\n"
                        + " ) ENGINE=InnoDB DEFAULT CHARSET=utf8";

        MySqlSourceConfigFactory configFactory = new MySqlSourceConfigFactory();
        configFactory.udalShardkeyColumn("acct_id");
        configFactory.hostname("127.0.0.1");
        configFactory.username("root");
        configFactory.password("**");
        configFactory.databaseList("test");
        configFactory.tableList("test.tablename");
        configFactory.startupOptions(StartupOptions.initial());
        configFactory.serverTimeZone("Asia/Shanghai");
        Duration duration = Duration.ofSeconds(30);
        configFactory.connectTimeout(duration);
        Properties properties = new Properties();
        properties.setProperty("database.server.name", "test");
        configFactory.debeziumProperties(properties);

        MySqlSourceConfig sourceConfig = configFactory.createConfig(0);
        MySqlPipelineRecordEmitter mySqlPipelineRecordEmitter =
                new MySqlPipelineRecordEmitter(sourceConfig);
        TableId tableId = new TableId(null, "test", "tablename");
        Method method = null;
        try {
            method =
                    mySqlPipelineRecordEmitter
                            .getClass()
                            .getDeclaredMethod("parseDDL", String.class, TableId.class);
            method.setAccessible(true);
            Schema schema =
                    (Schema) method.invoke(mySqlPipelineRecordEmitter, createTableDdl, tableId);
            List<String> list = new ArrayList<>();
            list.add("column1");
            list.add("acct_id");
            assertThat(schema.primaryKeys()).isNotEmpty().isEqualTo(list);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
