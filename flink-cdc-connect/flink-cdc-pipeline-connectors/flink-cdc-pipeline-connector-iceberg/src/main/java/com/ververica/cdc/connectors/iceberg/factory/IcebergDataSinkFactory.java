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

package com.ververica.cdc.connectors.iceberg.factory;

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.configuration.ConfigOption;
import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.factories.DataSinkFactory;
import com.ververica.cdc.common.pipeline.PipelineOptions;
import com.ververica.cdc.common.sink.DataSink;
import com.ververica.cdc.connectors.iceberg.sink.IcebergDataSink;

import java.time.ZoneId;
import java.util.HashSet;
import java.util.Set;

import static com.ververica.cdc.connectors.iceberg.sink.IcebergDataSinkOptions.CATALOG_TYPE;
import static com.ververica.cdc.connectors.iceberg.sink.IcebergDataSinkOptions.CLIENT_POOL_SIZE;
import static com.ververica.cdc.connectors.iceberg.sink.IcebergDataSinkOptions.DATABASE;
import static com.ververica.cdc.connectors.iceberg.sink.IcebergDataSinkOptions.FSDEFAULTFS;
import static com.ververica.cdc.connectors.iceberg.sink.IcebergDataSinkOptions.HDFS_CACHE;
import static com.ververica.cdc.connectors.iceberg.sink.IcebergDataSinkOptions.HIVECATALOG;
import static com.ververica.cdc.connectors.iceberg.sink.IcebergDataSinkOptions.HIVEURI;
import static com.ververica.cdc.connectors.iceberg.sink.IcebergDataSinkOptions.OVERWRITE_MODE;
import static com.ververica.cdc.connectors.iceberg.sink.IcebergDataSinkOptions.SINK_PARALLELISM;
import static com.ververica.cdc.connectors.iceberg.sink.IcebergDataSinkOptions.TABLENAME;
import static com.ververica.cdc.connectors.iceberg.sink.IcebergDataSinkOptions.WAREHOUSE;

/** A dummy {@link DataSinkFactory} to create {@link IcebergDataSink}. */
@Internal
public class IcebergDataSinkFactory implements DataSinkFactory {
    @Override
    public DataSink createDataSink(Context context) {
        Configuration config = context.getFactoryConfiguration();
        return new IcebergDataSink(
                config,
                ZoneId.of(
                        context.getPipelineConfiguration()
                                .get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE)));
    }

    @Override
    public String identifier() {
        return "iceberg";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DATABASE);
        options.add(TABLENAME);
        options.add(WAREHOUSE);
        options.add(HIVEURI);
        options.add(FSDEFAULTFS);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(CATALOG_TYPE);
        options.add(SINK_PARALLELISM);
        options.add(HIVECATALOG);
        options.add(CLIENT_POOL_SIZE);
        options.add(HDFS_CACHE);
        options.add(OVERWRITE_MODE);
        return options;
    }
}
