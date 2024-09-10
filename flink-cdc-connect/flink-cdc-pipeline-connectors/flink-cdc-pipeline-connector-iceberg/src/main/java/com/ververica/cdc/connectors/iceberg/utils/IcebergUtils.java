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

package com.ververica.cdc.connectors.iceberg.utils;

import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.connectors.iceberg.sink.IcebergDataSinkOptions;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkCatalogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/** Supports {@link IcebergUtils} to hive catalog. */
public class IcebergUtils {
    private static final Logger LOG = LoggerFactory.getLogger(IcebergUtils.class);

    public static org.apache.hadoop.conf.Configuration hadoopConfiguration(Configuration config) {
        org.apache.hadoop.conf.Configuration configuration =
                new org.apache.hadoop.conf.Configuration();
        configuration.set("fs.defaultFS", config.get(IcebergDataSinkOptions.FSDEFAULTFS));
        configuration.setBoolean(
                "fs.hdfs.impl.disable.cache", config.get(IcebergDataSinkOptions.HDFS_CACHE));
        String prefix = "option.catalog.hdfs.";
        HashMap<String, String> map = new HashMap<>();
        getConfigMap(map, prefix, config);
        for (Object key : map.keySet()) {
            configuration.set(key.toString(), map.get(key));
        }
        return configuration;
    }

    public static CatalogLoader catalogLoader(String catalog, Configuration config) {
        String catalogType = config.get(IcebergDataSinkOptions.CATALOG_TYPE);
        HashMap<String, String> map = new HashMap<>();
        map.put("type", "iceberg");
        switch (catalogType) {
            case FlinkCatalogFactory.ICEBERG_CATALOG_TYPE_HIVE:
                map.put(
                        FlinkCatalogFactory.ICEBERG_CATALOG_TYPE,
                        FlinkCatalogFactory.ICEBERG_CATALOG_TYPE_HIVE);
                map.put(
                        CatalogProperties.WAREHOUSE_LOCATION,
                        config.get(IcebergDataSinkOptions.WAREHOUSE));
                map.put(CatalogProperties.URI, config.get(IcebergDataSinkOptions.HIVEURI));
                map.put(
                        CatalogProperties.CLIENT_POOL_SIZE,
                        config.get(IcebergDataSinkOptions.CLIENT_POOL_SIZE));
                String prefix = "option.catalog.hive.";
                getConfigMap(map, prefix, config);
                return CatalogLoader.hive(catalog, hadoopConfiguration(config), map);
            case FlinkCatalogFactory.ICEBERG_CATALOG_TYPE_HADOOP:
                LOG.debug("Hadoop catalog dont not need init catalogloader!");
                return null;
            default:
                LOG.debug("this type catalog dont not need init catalogloader!");
                return null;
        }
    }

    private static void getConfigMap(
            HashMap<String, String> map, String prefix, Configuration config) {
        Map configMap = config.toMap();
        for (Object key : configMap.keySet()) {
            String catalogKey = key.toString();
            if (key.toString().startsWith(prefix)) {
                int index = catalogKey.indexOf(prefix);
                if (index != -1) {
                    catalogKey = catalogKey.substring(index + prefix.length());
                }
                map.put(catalogKey, configMap.get(key).toString());
            }
        }
    }
}
