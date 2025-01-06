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

package com.ververica.cdc.connectors.oracle.factory;

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.configuration.ConfigOption;
import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.factories.DataSourceFactory;
import com.ververica.cdc.common.factories.Factory;
import com.ververica.cdc.common.source.DataSource;
import com.ververica.cdc.connectors.oracle.source.TidbDataSource;
import com.ververica.cdc.connectors.oracle.source.TidbDataSourceOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/** A {@link Factory} to create {@link TidbDataSource}. */
@Internal
public class TidbDataSourceFactory implements DataSourceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(TidbDataSourceFactory.class);

    public static final String IDENTIFIER = "tidb";

    @Override
    public DataSource createDataSource(Context context) {
        final Configuration config = context.getFactoryConfiguration();
        return new TidbDataSource(config);
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(TidbDataSourceOptions.HOSTNAME);
        options.add(TidbDataSourceOptions.PD_PORT);
        options.add(TidbDataSourceOptions.TIDB_PORT);
        options.add(TidbDataSourceOptions.USERNAME);
        options.add(TidbDataSourceOptions.PASSWORD);
        options.add(TidbDataSourceOptions.DATABASE);
        options.add(TidbDataSourceOptions.TLS_CERT_FILE);
        options.add(TidbDataSourceOptions.TLS_KEY_FILE);
        options.add(TidbDataSourceOptions.TLS_CHAINFILE);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(TidbDataSourceOptions.SERVER_TIME_ZONE);
        options.add(TidbDataSourceOptions.SCAN_STARTUP_MODE);
        return options;
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
