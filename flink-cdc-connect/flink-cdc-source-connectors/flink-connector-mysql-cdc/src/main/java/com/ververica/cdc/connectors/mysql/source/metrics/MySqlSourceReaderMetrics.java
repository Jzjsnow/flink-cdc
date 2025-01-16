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

package com.ververica.cdc.connectors.mysql.source.metrics;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.MetricNames;

import com.ververica.cdc.connectors.mysql.source.reader.MySqlSourceReader;

/** A collection class for handling metrics in {@link MySqlSourceReader}. */
public class MySqlSourceReaderMetrics {

    private final MetricGroup metricGroup;
    private static final String METRIC_NAME_FORMAT = "%s_%s";
    private static final String FLINK_CDC_SOURCE_MYSQL = "flinkCDC_Source_Mysql";
    private static final String IS_SNAPSHOT_SPLIT_STATE = "isSnapshotSplitState";

    /**
     * currentFetchEventTimeLag = FetchTime - messageTimestamp, where the FetchTime is the time the
     * record fetched into the source operator.
     */
    private volatile long fetchDelay = 0L;

    /**
     * numRecordsIn, typically represents the number of MySQL records that have been sent or
     * transmitted.
     */
    private volatile double numRecordsIn = 0L;

    /**
     * numBytesIn, typically represents the bytes of MySQL records that have been sent or
     * transmitted.
     */
    private volatile double numBytesIn = 0L;

    /**
     * numRecordsInRate, typically represents the rate of MySQL records that are being received or
     * transmitted over a specified period, calculated as the change in the number of received
     * records divided by the time interval.
     */
    private volatile double numRecordsInRate = 0L;

    /**
     * numBytesInRate, typically represents the rate of bytes of MySQL records that are being
     * received or transmitted over a specified period, calculated as the change in the total size
     * of received records divided by the time interval.
     */
    private volatile double numBytesInRate = 0L;

    /**
     * isSnapshotSplitState, typically represents the stage of data synchronization about data
     * source. If this metric is 0L, the data synchronization is in the synchronization stage of
     * snapshot. If this metric is 1L, the data synchronization parameter is in the incremental log
     * synchronization phase.
     */
    private volatile long isSnapshotSplitState = 0L;

    public MySqlSourceReaderMetrics(MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
    }

    public void registerMetrics() {
        metricGroup.gauge(
                String.format(
                        METRIC_NAME_FORMAT,
                        FLINK_CDC_SOURCE_MYSQL,
                        MetricNames.CURRENT_FETCH_EVENT_TIME_LAG),
                (Gauge<Long>) this::getFetchDelay);
        metricGroup.gauge(
                String.format(
                        METRIC_NAME_FORMAT, FLINK_CDC_SOURCE_MYSQL, MetricNames.IO_NUM_RECORDS_IN),
                (Gauge<Double>) this::getNumRecordsIn);
        metricGroup.gauge(
                String.format(
                        METRIC_NAME_FORMAT, FLINK_CDC_SOURCE_MYSQL, MetricNames.IO_NUM_BYTES_IN),
                (Gauge<Double>) this::getNumBytesIn);
        metricGroup.gauge(
                String.format(
                        METRIC_NAME_FORMAT,
                        FLINK_CDC_SOURCE_MYSQL,
                        MetricNames.IO_NUM_RECORDS_IN_RATE),
                (Gauge<Double>) this::getNumRecordsInRate);
        metricGroup.gauge(
                String.format(
                        METRIC_NAME_FORMAT,
                        FLINK_CDC_SOURCE_MYSQL,
                        MetricNames.IO_NUM_BYTES_IN_RATE),
                (Gauge<Double>) this::getNumBytesInRate);
        metricGroup.gauge(
                String.format(METRIC_NAME_FORMAT, FLINK_CDC_SOURCE_MYSQL, IS_SNAPSHOT_SPLIT_STATE),
                (Gauge<Long>) this::getIsSnapshotSplitState);
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

    public long getIsSnapshotSplitState() {
        return isSnapshotSplitState;
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

    public void recordIsSnapshotSplitState(long isSnapshotSplitState) {
        this.isSnapshotSplitState = isSnapshotSplitState;
    }
}
