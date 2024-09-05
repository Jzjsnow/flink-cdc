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

package com.ververica.cdc.connectors.hudi.factory;

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.configuration.ConfigOption;
import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.factories.DataSinkFactory;
import com.ververica.cdc.common.sink.DataSink;
import com.ververica.cdc.connectors.hudi.sink.HudiDataSink;

import java.util.HashSet;
import java.util.Set;

import static com.ververica.cdc.connectors.hudi.sink.HudiDataSinkOptions.*;

/** A dummy {@link DataSinkFactory} to create {@link HudiDataSink}. */
@Internal
public class HudiDataSinkFactory implements DataSinkFactory {
    @Override
    public DataSink createDataSink(Context context) {
        Configuration config = context.getFactoryConfiguration();
        return new HudiDataSink(config);
    }

    @Override
    public String identifier() {
        return "hudi";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(TABLENAME);
        options.add(TABLEPATH);
        options.add(PRIMARYKEY);
        options.add(PARTITIONFIELD);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(WRITE_OPERATION_MODE);
        options.add(READ_STREAMING_CHECK_INTERVAL);
        options.add(READ_AS_STREAMING);
        options.add(HIVE_SYNC_ENABLED);
        options.add(DATASOURCE_WRITE_RECONCILE_SCHEMA);
        options.add(SCHEMA_ON_READ_ENABLE);
        options.add(CLEANER_COMMITS_RETAINED);
        options.add(INDEX_TYPE);
        options.add(WRITE_TASKS_NUM);
        options.add(COMPACTION_TASKS_NUM);
        options.add(SERVER_TIME_ZONE);
        options.add(KERBEROS_PRINCIPAL_PATTERN);
        options.add(USE_DATANODE_HOSTNAME);
        options.add(HDFS_CACHE);
        return options;
    }
}
