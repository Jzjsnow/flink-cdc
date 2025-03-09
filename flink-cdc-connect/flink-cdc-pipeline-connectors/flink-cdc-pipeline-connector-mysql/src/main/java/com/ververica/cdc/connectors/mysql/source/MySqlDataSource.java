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

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.annotation.VisibleForTesting;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.source.DataSource;
import com.ververica.cdc.common.source.EventSourceProvider;
import com.ververica.cdc.common.source.FlinkSourceProvider;
import com.ververica.cdc.common.source.MetadataAccessor;
import com.ververica.cdc.common.source.SupportedMetadataColumn;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import com.ververica.cdc.connectors.mysql.source.reader.MySqlPipelineRecordEmitter;
import com.ververica.cdc.connectors.mysql.table.MySqlReadableMetadata;
import com.ververica.cdc.debezium.table.DebeziumChangelogMode;

import java.util.ArrayList;
import java.util.List;

/** A {@link DataSource} for mysql cdc connector. */
@Internal
public class MySqlDataSource implements DataSource {

    private final MySqlSourceConfigFactory configFactory;
    private final MySqlSourceConfig sourceConfig;
    private final List<MySqlReadableMetadata> readableMetadataList;

    public MySqlDataSource(MySqlSourceConfigFactory configFactory) {
        this(configFactory, new ArrayList<>());
    }

    public MySqlDataSource(
            MySqlSourceConfigFactory configFactory,
            List<MySqlReadableMetadata> readableMetadataList) {
        this.configFactory = configFactory;
        this.sourceConfig = configFactory.createConfig(0);
        this.readableMetadataList = readableMetadataList;
    }

    @Override
    public EventSourceProvider getEventSourceProvider() {
        MySqlEventDeserializer deserializer =
                new MySqlEventDeserializer(
                        DebeziumChangelogMode.ALL,
                        sourceConfig.isIncludeSchemaChanges(),
                        sourceConfig.getServerTimeZone(),
                        sourceConfig.getHostname(),
                        String.valueOf(sourceConfig.getPort()),
                        sourceConfig.isAddMeta(),
                        readableMetadataList);

        MySqlSource<Event> source =
                new MySqlSource<>(
                        configFactory,
                        deserializer,
                        (sourceReaderMetrics, sourceConfig) ->
                                new MySqlPipelineRecordEmitter(
                                        deserializer, sourceReaderMetrics, sourceConfig));

        return FlinkSourceProvider.of(source);
    }

    @Override
    public MetadataAccessor getMetadataAccessor() {
        return new MySqlMetadataAccessor(sourceConfig);
    }

    @VisibleForTesting
    public MySqlSourceConfig getSourceConfig() {
        return sourceConfig;
    }

    @Override
    public SupportedMetadataColumn[] supportedMetadataColumns() {
        if (readableMetadataList.contains(MySqlReadableMetadata.OP_TS)) {
            return new SupportedMetadataColumn[] {new OpTsMetadataColumn()};
        } else {
            return new SupportedMetadataColumn[0];
        }
    }
}
