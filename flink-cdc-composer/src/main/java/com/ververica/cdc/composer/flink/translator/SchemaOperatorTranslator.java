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

package com.ververica.cdc.composer.flink.translator;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.pipeline.SchemaChangeBehavior;
import com.ververica.cdc.common.sink.MetadataApplier;
import com.ververica.cdc.composer.definition.RouteDef;
import com.ververica.cdc.runtime.operators.schema.SchemaOperatorFactory;
import com.ververica.cdc.runtime.typeutils.EventTypeInfo;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Translator for building {@link com.ververica.cdc.runtime.operators.schema.SchemaOperator} into
 * DataStream.
 */
@Internal
public class SchemaOperatorTranslator {
    private final SchemaChangeBehavior schemaChangeBehavior;
    private final String schemaOperatorUid;
    private final Duration rpcTimeOut;

    public SchemaOperatorTranslator(
            SchemaChangeBehavior schemaChangeBehavior,
            String schemaOperatorUid,
            Duration rpcTimeOut) {
        this.schemaChangeBehavior = schemaChangeBehavior;
        this.schemaOperatorUid = schemaOperatorUid;
        this.rpcTimeOut = rpcTimeOut;
    }

    public DataStream<Event> translate(
            DataStream<Event> input,
            int parallelism,
            MetadataApplier metadataApplier,
            List<RouteDef> routes) {
        return addSchemaOperator(input, parallelism, metadataApplier, schemaChangeBehavior, routes);
    }

    public String getSchemaOperatorUid() {
        return schemaOperatorUid;
    }

    private DataStream<Event> addSchemaOperator(
            DataStream<Event> input,
            int parallelism,
            MetadataApplier metadataApplier,
            SchemaChangeBehavior schemaChangeBehavior,
            List<RouteDef> routes) {
        List<Tuple2<String, TableId>> routingRules = new ArrayList<>();
        for (RouteDef route : routes) {
            routingRules.add(
                    Tuple2.of(route.getSourceTable(), TableId.parse(route.getSinkTable())));
        }
        SingleOutputStreamOperator<Event> stream =
                input.transform(
                        "SchemaOperator",
                        new EventTypeInfo(),
                        new SchemaOperatorFactory(
                                metadataApplier, rpcTimeOut, schemaChangeBehavior, routingRules));
        stream.uid(schemaOperatorUid).setParallelism(parallelism);
        return stream;
    }

    private DataStream<Event> dropSchemaChangeEvent(DataStream<Event> input, int parallelism) {
        return input.filter(event -> !(event instanceof SchemaChangeEvent))
                .setParallelism(parallelism);
    }

    private DataStream<Event> exceptionOnSchemaChange(DataStream<Event> input, int parallelism) {
        return input.map(
                        event -> {
                            if (event instanceof SchemaChangeEvent) {
                                throw new RuntimeException(
                                        String.format(
                                                "Aborting execution as the pipeline encountered a schema change event: %s",
                                                event));
                            }
                            return event;
                        })
                .setParallelism(parallelism);
    }
}
