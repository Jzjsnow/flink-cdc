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

package com.ververica.cdc.runtime.operators.schema.coordinator;

import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Selectors;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.pipeline.SchemaChangeBehavior;
import com.ververica.cdc.common.sink.MetadataApplier;

import java.util.List;
import java.util.stream.Collectors;

/** Provider of {@link SchemaRegistry}. */
@Internal
public class SchemaRegistryProvider implements OperatorCoordinator.Provider {
    private static final long serialVersionUID = 1L;

    private final OperatorID operatorID;
    private final String operatorName;
    private final MetadataApplier metadataApplier;
    private final SchemaChangeBehavior schemaChangeBehavior;
    private final List<Tuple2<String, TableId>> routingRules;

    public SchemaRegistryProvider(
            OperatorID operatorID,
            String operatorName,
            MetadataApplier metadataApplier,
            SchemaChangeBehavior schemaChangeBehavior,
            List<Tuple2<String, TableId>> routingRules) {
        this.operatorID = operatorID;
        this.operatorName = operatorName;
        this.metadataApplier = metadataApplier;
        this.schemaChangeBehavior = schemaChangeBehavior;
        this.routingRules = routingRules;
    }

    @Override
    public OperatorID getOperatorId() {
        return operatorID;
    }

    @Override
    public OperatorCoordinator create(OperatorCoordinator.Context context) throws Exception {
        List<Tuple2<Selectors, TableId>> routes =
                routingRules.stream()
                        .map(
                                tuple2 -> {
                                    String tableInclusions = tuple2.f0;
                                    TableId replaceBy = tuple2.f1;
                                    Selectors selectors =
                                            new Selectors.SelectorsBuilder()
                                                    .includeTables(tableInclusions)
                                                    .build();
                                    return new Tuple2<>(selectors, replaceBy);
                                })
                        .collect(Collectors.toList());
        return new SchemaRegistry(operatorName, context, metadataApplier, schemaChangeBehavior, routes);
    }
}
