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

package com.ververica.cdc.common.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;

/**
 * Base interface for developing a sink. A basic {@link org.apache.flink.api.connector.sink2.Sink}
 * is a stateless sink that can flush data on checkpoint to achieve at-least-once consistency. Sinks
 * with additional requirements should implement {@link StatefulSink} or {@link
 * TwoPhaseCommittingSink}.
 *
 * <p>The {@link org.apache.flink.api.connector.sink2.Sink} needs to be serializable. All
 * configuration should be validated eagerly. The respective sink writers are transient and will
 * only be created in the subtasks on the taskmanagers.
 *
 * @param <Event> The type of the sink's input
 */
@PublicEvolving
public interface CustomSink<Event> extends Serializable {

    /**
     * Creates a {@link SinkWriter}.
     *
     * @param dataStream the runtime context.
     * @return
     */
    void sinkTo(DataStream<Event> dataStream, OperatorID schemaOperatorId);
}
