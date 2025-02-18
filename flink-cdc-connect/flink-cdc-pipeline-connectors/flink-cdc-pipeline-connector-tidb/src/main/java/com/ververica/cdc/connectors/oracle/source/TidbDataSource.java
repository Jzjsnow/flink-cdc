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

package com.ververica.cdc.connectors.oracle.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.DataTypeUtils;

import com.esotericsoftware.minlog.Log;
import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.source.DataSource;
import com.ververica.cdc.common.source.EventSourceProvider;
import com.ververica.cdc.common.source.FlinkSourceFunctionProvider;
import com.ververica.cdc.common.source.MetadataAccessor;
import com.ververica.cdc.connectors.oracle.dto.JdbcInfo;
import com.ververica.cdc.connectors.oracle.source.reader.TiDBSourceReader;
import com.ververica.cdc.connectors.oracle.utils.SchemaUtils;
import com.ververica.cdc.connectors.tidb.TDBSourceOptions;
import com.ververica.cdc.connectors.tidb.table.StartupOptions;
import com.ververica.cdc.connectors.tidb.table.TiKVMetadataConverter;
import org.tikv.common.TiConfiguration;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ververica.cdc.connectors.oracle.source.TidbDataSourceOptions.SCAN_STARTUP_MODE;

/** A {@link DynamicTableSource} description. */
public class TidbDataSource implements DataSource {

    private static final String SCAN_STARTUP_MODE_VALUE_INITIAL = "initial";
    private static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";
    /** Data type that describes the final output of the source. */
    protected DataType producedDataType;

    /** Metadata that is appended at the end of a physical source row. */
    protected List<String> metadataKeys;

    private Configuration config;

    public TidbDataSource(Configuration config) {
        this.config = config;
        this.metadataKeys = Collections.emptyList();
    }

    @Override
    public EventSourceProvider getEventSourceProvider() {
        String hostname = config.get(TidbDataSourceOptions.HOSTNAME);
        int pdPort = config.get(TidbDataSourceOptions.PD_PORT);
        int tidbPort = config.get(TidbDataSourceOptions.TIDB_PORT);
        String username = config.get(TidbDataSourceOptions.USERNAME);
        String password = config.get(TidbDataSourceOptions.PASSWORD);
        List<String> tables = Arrays.asList(config.get(TidbDataSourceOptions.TABLES));
        int snapshotWorkerNums = config.get(TidbDataSourceOptions.SNAPSHOT_WORKER_NUMS);

        JdbcInfo jdbcInfo = new JdbcInfo();
        jdbcInfo.setPassword(password);
        jdbcInfo.setDatabase(
                "INFORMATION_SCHEMA"); // tidb system database,as the database of the jdbc url
        jdbcInfo.setHost(hostname);
        jdbcInfo.setPort(tidbPort);
        jdbcInfo.setUsername(username);
        List<TableId> capturedTableIds = SchemaUtils.getCapturedTableIds(tables, jdbcInfo);

        String keyFile = config.get(TidbDataSourceOptions.TLS_KEY_FILE);
        String certFile = config.get(TidbDataSourceOptions.TLS_CERT_FILE);
        String chainFile = config.get(TidbDataSourceOptions.TLS_CHAINFILE);
        String sourceTimeZone = config.get(TidbDataSourceOptions.SERVER_TIME_ZONE);
        String ip = config.get(TidbDataSourceOptions.HOSTNAME);
        Map<String, RowType> physicalDataTypes = new HashMap<>();
        ResolvedSchema schema = null;
        for (TableId capturedTableId : capturedTableIds) {
            Log.info(
                    "capturedTables: "
                            + capturedTableId.getSchemaName()
                            + "."
                            + capturedTableId.getTableName());
            TableId tableId =
                    TableId.parse(
                            capturedTableId.getSchemaName() + "." + capturedTableId.getTableName());
            schema = SchemaUtils.getResolvedSchema(tableId, jdbcInfo);
            RowType physicalDataType = (RowType) schema.toPhysicalRowDataType().getLogicalType();
            physicalDataTypes.put(
                    capturedTableId.getSchemaName() + "." + capturedTableId.getTableName(),
                    physicalDataType);
        }
        TiKVMetadataConverter[] metadataConverters = getMetadataConverters();
        DataType producedDataType = schema.toPhysicalRowDataType();
        TypeInformation<Event> typeInfo =
                (TypeInformation<Event>) createTypeInformation(producedDataType);
        Map<String, String> options = new HashMap<>();
        TiConfiguration tfconf =
                TDBSourceOptions.getTiConfiguration("https://" + ip + ":" + pdPort, options);
        tfconf.setTlsEnable(true);
        tfconf.setKeyFile(keyFile);
        tfconf.setTrustCertCollectionFile(certFile);
        tfconf.setKeyCertChainFile(chainFile);
        TidbEventDeserializer seriliazer =
                new TidbEventDeserializer(
                        tfconf,
                        typeInfo,
                        metadataConverters,
                        physicalDataTypes,
                        jdbcInfo,
                        capturedTableIds);
        TidbSnapshotEventDeserializer snapshotEventDeserializer =
                new TidbSnapshotEventDeserializer(
                        tfconf,
                        typeInfo,
                        metadataConverters,
                        physicalDataTypes,
                        sourceTimeZone,
                        jdbcInfo,
                        capturedTableIds);
        SourceFunction<Event> tidbSource =
                TiDBSourceReader.<Event>builder()
                        .tiConf(tfconf)
                        .snapshotEventDeserializer(snapshotEventDeserializer)
                        .changeEventDeserializer(seriliazer)
                        .startupOptions(getStartupOptions(config))
                        .jdbcInfo(jdbcInfo)
                        .tables(capturedTableIds)
                        .snapshotWorkerNums(snapshotWorkerNums)
                        .build();
        return FlinkSourceFunctionProvider.of(tidbSource);
    }

    public TypeInformation<?> createTypeInformation(DataType producedDataType) {
        DataTypeUtils.validateInputDataType(producedDataType);
        return InternalTypeInfo.of(producedDataType.getLogicalType());
    }

    private static StartupOptions getStartupOptions(Configuration config) {
        String modeString = config.get(SCAN_STARTUP_MODE);

        switch (modeString.toLowerCase()) {
            case SCAN_STARTUP_MODE_VALUE_INITIAL:
                return StartupOptions.initial();

            case SCAN_STARTUP_MODE_VALUE_LATEST:
                return StartupOptions.latest();

            default:
                throw new ValidationException(
                        String.format(
                                "Invalid value for option '%s'. Supported values are [%s, %s], but was: %s",
                                SCAN_STARTUP_MODE.key(),
                                SCAN_STARTUP_MODE_VALUE_INITIAL,
                                SCAN_STARTUP_MODE_VALUE_LATEST,
                                modeString));
        }
    }

    @Override
    public MetadataAccessor getMetadataAccessor() {
        return new TidbMetadataAccessor(config);
    }

    private TiKVMetadataConverter[] getMetadataConverters() {
        return new TiKVMetadataConverter[0];
    }
}
