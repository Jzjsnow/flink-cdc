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

package com.ververica.cdc.connectors.base.source.metrics;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.runtime.metrics.MetricNames;

import com.ververica.cdc.connectors.base.source.reader.IncrementalSourceReader;

/** A collection class for handling metrics in {@link IncrementalSourceReader}. */
public class SourceReaderMetrics {

    private final SourceReaderMetricGroup metricGroup;
    private String dataSourceType;
    private static final String METRIC_NAME_FORMAT = "%s_%s";
    private static final String FLINK_CDC_SOURCE_POSTGRES = "flinkCDC_Source_Postgres";
    private static final String FLINK_CDC_SOURCE_ORACLE = "flinkCDC_Source_Oracle";

    /**
     * currentFetchEventTimeLag = FetchTime - messageTimestamp, where the FetchTime is the time the
     * record fetched into the source operator.
     */
    private volatile long fetchDelay = 0L;

    /** The total number of record that failed to consume, process or emit. */
    private final Counter numRecordsInErrorsCounter;

    /**
     * numRecordsIn, typically represents the number of postgres records that have been sent or
     * transmitted.
     */
    private volatile double numRecordsIn = 0L;

    /**
     * numBytesIn, typically represents the bytes of postgres records that have been sent or
     * transmitted.
     */
    private volatile double numBytesIn = 0L;

    /**
     * numRecordsInRate, typically represents the rate of postgres records that are being received
     * or transmitted over a specified period, calculated as the change in the number of received
     * records divided by the time interval.
     */
    private volatile double numRecordsInRate = 0L;

    /**
     * numBytesInRate, typically represents the rate of bytes of postgres records that are being
     * received or transmitted over a specified period, calculated as the change in the total size
     * of received records divided by the time interval.
     */
    private volatile double numBytesInRate = 0L;

    public SourceReaderMetrics(SourceReaderMetricGroup metricGroup, String dataSourceType) {
        this.metricGroup = metricGroup;
        this.numRecordsInErrorsCounter = metricGroup.getNumRecordsInErrorsCounter();
        this.dataSourceType = dataSourceType;
    }

    public void registerMetrics() {
        if ("Public".equals(dataSourceType)) {
            metricGroup.gauge(
                    MetricNames.CURRENT_FETCH_EVENT_TIME_LAG, (Gauge<Long>) this::getFetchDelay);
        } else if ("Oracle".equals(dataSourceType)) {
            metricGroup.gauge(
                    String.format(
                            METRIC_NAME_FORMAT,
                            FLINK_CDC_SOURCE_ORACLE,
                            MetricNames.IO_NUM_RECORDS_IN),
                    (Gauge<Double>) this::getNumRecordsIn);
            metricGroup.gauge(
                    String.format(
                            METRIC_NAME_FORMAT,
                            FLINK_CDC_SOURCE_ORACLE,
                            MetricNames.IO_NUM_BYTES_IN),
                    (Gauge<Double>) this::getNumBytesIn);
            metricGroup.gauge(
                    String.format(
                            METRIC_NAME_FORMAT,
                            FLINK_CDC_SOURCE_ORACLE,
                            MetricNames.IO_NUM_RECORDS_IN_RATE),
                    (Gauge<Double>) this::getNumRecordsInRate);
            metricGroup.gauge(
                    String.format(
                            METRIC_NAME_FORMAT,
                            FLINK_CDC_SOURCE_ORACLE,
                            MetricNames.IO_NUM_BYTES_IN_RATE),
                    (Gauge<Double>) this::getNumBytesInRate);
        } else if ("Postgres".equals(dataSourceType)) {
            metricGroup.gauge(
                    String.format(
                            METRIC_NAME_FORMAT,
                            FLINK_CDC_SOURCE_POSTGRES,
                            MetricNames.IO_NUM_RECORDS_IN),
                    (Gauge<Double>) this::getNumRecordsIn);
            metricGroup.gauge(
                    String.format(
                            METRIC_NAME_FORMAT,
                            FLINK_CDC_SOURCE_POSTGRES,
                            MetricNames.IO_NUM_BYTES_IN),
                    (Gauge<Double>) this::getNumBytesIn);
            metricGroup.gauge(
                    String.format(
                            METRIC_NAME_FORMAT,
                            FLINK_CDC_SOURCE_POSTGRES,
                            MetricNames.IO_NUM_RECORDS_IN_RATE),
                    (Gauge<Double>) this::getNumRecordsInRate);
            metricGroup.gauge(
                    String.format(
                            METRIC_NAME_FORMAT,
                            FLINK_CDC_SOURCE_POSTGRES,
                            MetricNames.IO_NUM_BYTES_IN_RATE),
                    (Gauge<Double>) this::getNumBytesInRate);
        }
    }

    public long getFetchDelay() {
        return fetchDelay;
    }

    public double getNumRecordsIn() {
        return numRecordsIn;
    }

    public double getNumBytesIn() {
        return numBytesIn;
    }

    public double getNumRecordsInRate() {
        return numRecordsInRate;
    }

    public double getNumBytesInRate() {
        return numBytesInRate;
    }

    public void recordFetchDelay(long fetchDelay) {
        this.fetchDelay = fetchDelay;
    }

    public void recordNumRecordsIn(double numRecordsIn) {
        this.numRecordsIn = numRecordsIn;
    }

    public void recordNumBytesIn(double numBytesIn) {
        this.numBytesIn = numBytesIn;
    }

    public void recordNumRecordsInRate(double numRecordsInRate) {
        this.numRecordsInRate = numRecordsInRate;
    }

    public void recordNumBytesInRate(double numBytesInRate) {
        this.numBytesInRate = numBytesInRate;
    }

    public void addNumRecordsInErrors(long delta) {
        this.numRecordsInErrorsCounter.inc(delta);
    }
}
