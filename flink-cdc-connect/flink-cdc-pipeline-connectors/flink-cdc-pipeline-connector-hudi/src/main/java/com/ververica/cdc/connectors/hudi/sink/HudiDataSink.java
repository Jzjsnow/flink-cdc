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

package com.ververica.cdc.connectors.hudi.sink;

import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.sink.CustomFlinkSinkProvider;
import com.ververica.cdc.common.sink.DataSink;
import com.ververica.cdc.common.sink.EventSinkProvider;
import com.ververica.cdc.common.sink.MetadataApplier;

import java.io.Serializable;

/** A {@link DataSink} for "hudi" connector. */
public class HudiDataSink implements DataSink, Serializable {

    private Configuration config;

    public HudiDataSink(Configuration config) {
        this.config = config;
    }

    @Override
    public EventSinkProvider getEventSinkProvider() {
        return CustomFlinkSinkProvider.of(new HudiSink(this.config));
    }

    @Override
    public MetadataApplier getMetadataApplier() {
        return new HudiMetadataApplier(config);
    }
}
