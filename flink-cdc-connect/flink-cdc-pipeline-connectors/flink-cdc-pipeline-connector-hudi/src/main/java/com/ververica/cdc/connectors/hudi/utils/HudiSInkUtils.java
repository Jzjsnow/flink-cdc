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

package com.ververica.cdc.connectors.hudi.utils;

import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.connectors.hudi.sink.HudiDataSinkOptions;
import org.apache.hudi.common.table.HoodieTableMetaClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ververica.cdc.connectors.hudi.sink.HudiMetadataApplier.getMetaClient;

/** A {@link HudiSInkUtils} for "hudi" connector. */
public class HudiSInkUtils {

    public static HoodieTableMetaClient getHoodieTableMetaClient(Configuration config) {
        final List<String> locations = new ArrayList<>();
        org.apache.hadoop.conf.Configuration hadoopConf =
                new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("fs.hdfs.impl.disable.cache", config.get(HudiDataSinkOptions.HDFS_CACHE));
        hadoopConf.set("dfs.client.use.datanode.hostname", config.get(HudiDataSinkOptions.USE_DATANODE_HOSTNAME));
        hadoopConf.set("dfs.namenode.kerberos.principal.pattern", config.get(HudiDataSinkOptions.KERBEROS_PRINCIPAL_PATTERN));
        String prefix="option.hadoop.";
        HashMap<String, String> map = new HashMap<>();
        getConfigMap(map,prefix,config);
        String tablePath = config.getOptional(HudiDataSinkOptions.TABLEPATH).get();
        HoodieTableMetaClient hoodieTableMetaClient = getMetaClient(hadoopConf, tablePath);
        return hoodieTableMetaClient;
    }

    public static void getConfigMap(
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
