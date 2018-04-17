/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.citic.source.canal;

import com.citic.helper.RegexHashMap;
import com.citic.helper.Utility;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.*;

class CanalConf {
    private static final Logger LOGGER = LoggerFactory.getLogger(CanalConf.class);
    private String IPAddress;
    private String zkServers;
    private String destination;
    private String username;
    private String password;
    private int batchSize;
    private String serverUrl;
    private String serverUrls;

    // topic list
    private List<String> topicAppendList = Lists.newArrayList();
    private List<String> filterTableList = Lists.newArrayList();
    // db.table -> topic
    private Map<String, String> tableToTopicMap = new RegexHashMap<>();
    // topic -> schema
    private Map<String, String> topicToSchemaMap = Maps.newHashMap();

    // topic -> schema fields list
    private Map<String, List<String>> topicToSchemaFields = Maps.newHashMap();
    // topic,schema_field -> table_field
    private Table<String, String, String> topicSchemaFieldToTableField = HashBasedTable.create();

    /*
    * 设置表名和 topic 对应 map
    * */
    void setTableToTopicMap(String tableToTopicMap) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(tableToTopicMap), "tableToTopicMap cannot empty");
        // test.test:test123:schema1;test.test1:test234:schema2
        Splitter.on(';')
                .omitEmptyStrings()
                .trimResults()
                .split(tableToTopicMap)
                .forEach(item ->{
                    String[] result =  item.split(":");
                    Preconditions.checkArgument(result.length == 3,
                            "tableToTopicMap format incorrect eg: db.tbl1:topic1:schema1");

                    Preconditions.checkArgument(!Strings.isNullOrEmpty(result[0].trim()),
                            "db.table cannot empty");
                    Preconditions.checkArgument(!Strings.isNullOrEmpty(result[1].trim()),
                            "topic cannot empty");
                    Preconditions.checkArgument(!Strings.isNullOrEmpty(result[2].trim()),
                            "schema cannot empty");

                    filterTableList.add(result[0].trim());
                    topicAppendList.add(result[1].trim());

                    // db.table -> topic
                    this.tableToTopicMap.put(result[0].trim(), result[1].trim());
                    // topic -> avro schema
                    this.topicToSchemaMap.put(result[1].trim(), result[2].trim());
                });
    }

    /*
    * 设置表名与字段过滤对应 table
    * */
    void setTableFieldsFilter(String tableFieldsFilter) {
        if (Strings.isNullOrEmpty(tableFieldsFilter))
            return;
        // 这里表的顺序根据配置文件中 tableToTopicMap 表的顺序
        // id|id1,name|name1;uid|uid2,name|name2
        final int[] counter = {0};
        Splitter.on(';')
            .omitEmptyStrings()
            .trimResults()
            .split(tableFieldsFilter)
            .forEach(item -> {
                String topic = topicAppendList.get(counter[0]);
                counter[0] += 1;

                List<String> schemaFields = Lists.newArrayList();
                Splitter.on(",")
                    .omitEmptyStrings()
                    .trimResults()
                    .split(item)
                    .forEach(field -> {
                        String[] fieldTableSchema = field.split("\\|");
                        Preconditions.checkArgument(fieldTableSchema.length == 2,
                                "tableFieldsFilter 格式错误 eg: id|id1,name|name1");

                        Preconditions.checkArgument(!Strings.isNullOrEmpty(fieldTableSchema[0].trim()),
                                "table field cannot empty");
                        Preconditions.checkArgument(!Strings.isNullOrEmpty(fieldTableSchema[1].trim()),
                                "schema field cannot empty");

                        schemaFields.add(fieldTableSchema[1]);
                        this.topicSchemaFieldToTableField.put(topic, fieldTableSchema[1], fieldTableSchema[0]);

                    });
                topicToSchemaFields.put(topic, schemaFields);
            });
    }

    String getFromDBIP() {
        // destination example: 192_168_2_24-3306
        return this.destination.replace("-", ":").replace("_", ".");
    }

    /*
    * 根据表名获取 topic
    * */
    String getTableTopic(String schemaTableName) {
        if (this.tableToTopicMap != null)
            return this.tableToTopicMap.getOrDefault(schemaTableName, CanalSourceConstants.DEFAULT_NOT_MAP_TOPIC);
        else
            return CanalSourceConstants.DEFAULT_NOT_MAP_TOPIC;
    }

    void setZkServers(String zkServers) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(zkServers), "zkServers cannot empty");
        this.zkServers = zkServers;
    }

    void setDestination(String destination) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(destination), "destination cannot empty");
        this.destination = destination;
    }

    Map<String, String> getTopicToSchemaMap() { return topicToSchemaMap; }

    Map<String, List<String>> getTopicToSchemaFields() { return topicToSchemaFields;}

    String getIPAddress() { return IPAddress; };

    Map<String, String> getTableToTopicMap() { return tableToTopicMap; }

    Table<String, String, String> getTopicSchemaFieldToTableField() { return this.topicSchemaFieldToTableField; }

    String getZkServers() { return zkServers; }

    String getDestination() { return destination;}

    String getUsername() { return username; }

    String getPassword() { return password;}

    int getBatchSize() { return batchSize; }

    String getServerUrl() { return serverUrl; }

    String getServerUrls() { return serverUrls; }

    void setUsername(String username) { this.username = username;}

    void setPassword(String password) { this.password = password;}

    void setBatchSize(int batchSize) { this.batchSize = batchSize; }

    void setServerUrl(String serverUrl) { this.serverUrl = serverUrl; }

    void setIpInterface(String ipInterface) { IPAddress = Utility.getLocalIP(ipInterface); }

    void setServerUrls(String serverUrls) { this.serverUrls = serverUrls;}

    /*
    * 获取需要过滤的表列表
    * */
    String getTableFilter() {
        return Joiner.on(",").join(filterTableList);
    }

    boolean isConnectionUrlValid() {
        return !(Strings.isNullOrEmpty(this.zkServers)
                && Strings.isNullOrEmpty(this.serverUrl)
                && Strings.isNullOrEmpty(this.serverUrls));
    }

    static List<SocketAddress> convertUrlsToSocketAddressList(String serverUrls) throws
            ServerUrlsFormatException {
        List<SocketAddress> addresses = new ArrayList<>();
        if (StringUtils.isNotEmpty(serverUrls)) {
            for (String serverUrl : serverUrls.split(",")) {
                if (StringUtils.isNotEmpty(serverUrl)) {
                    try {
                        addresses.add(convertUrlToSocketAddress(serverUrl));
                    } catch (Exception exception) {
                        throw new ServerUrlsFormatException(String.format("The serverUrls are malformed. " +
                                "The ServerUrls : \"%s\" .", serverUrls), exception);
                    }
                }
            }
            return addresses;
        } else {
            return addresses;
        }
    }

    static SocketAddress convertUrlToSocketAddress(String serverUrl) throws ServerUrlsFormatException,
            NumberFormatException {
        String[] hostAndPort = serverUrl.split(":");
        if (hostAndPort.length == 2 && StringUtils.isNotEmpty(hostAndPort[1])) {
            int port  = Integer.parseInt(hostAndPort[1]);
            return new InetSocketAddress(hostAndPort[0], port);
        } else {
            throw new ServerUrlsFormatException(String.format("The serverUrl is malformed . The ServerUrl : \"%s\" .",
                    serverUrl));
        }
    }
}
