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

public class CanalConf {
    private static final Logger LOGGER = LoggerFactory.getLogger(CanalConf.class);

    private String ipInterface;
    private String zkServers;
    private String destination;
    private String username;
    private String password;
    private int batchSize;
    private String serverUrl;
    private String serverUrls;
    private Boolean oldDataRequired;

    private Map<String, String> tableToTopicMap = new RegexHashMap<>();
    private Table<String, String, Boolean> tableFieldsFilter = RegexHashBasedTable.create();
    private List<String> filterTableList = Lists.newArrayList();

    public String getIpInterface() {
        return ipInterface;
    }

    public void setIpInterface(String ipInterface) {
        this.ipInterface = ipInterface;
    }


    public String getFromDBIP() {
        // destination example: 192_168_2_24-3306
        return this.destination.replace("-", ":").replace("_", ".");
    }

    /*
    * 设置表名与字段过滤对应 table
    * */
    public void setTableFieldsFilter(String tableFieldsFilter) {
        if (Strings.isNullOrEmpty(tableFieldsFilter))
            return;
        // 这里表的顺序根据配置文件中 tableToTopicMap 表的顺序
        // field_name,field_name;field_name,field_name
        final int[] counter = {0};
        Splitter.on(';')
            .omitEmptyStrings()
            .trimResults()
            .split(tableFieldsFilter)
            .forEach(item -> {
                try {
                    String k = filterTableList.get(counter[0]);
                    counter[0] += 1;
                    Iterable<String> fieldList =
                            Splitter.on(",").omitEmptyStrings().trimResults().split(item);

                    for (String field : fieldList) {
                        this.tableFieldsFilter.put(k, field, true);
                    }
                } catch (IndexOutOfBoundsException e) {
                    throw new IndexOutOfBoundsException(
                            String.format("tableFieldsFilter 列表个数和 tableToTopicMap 不匹配. %s", e.getMessage()));
                }
            });
    }

    /*
    * 判断表名，字段是否在过滤列表中
    * */
    public boolean isFieldNeedOutput(String schemaTableName, String fieldName) {
        if (this.tableFieldsFilter != null) {
            /*
            * 这里的逻辑为,如果表没有配置字段过滤,则不对表做过滤,输出表的全部字段
            * 如果表配置了字段过滤,在只有配置中的字段会输出到最终结果
            * */
            return !this.tableFieldsFilter.containsRow(schemaTableName)
                    || this.tableFieldsFilter.containsColumn(fieldName);
        } else {
            return true;
        }
    }

    /*
    * 设置表名和 topic 对应 map
    * */
    public void setTableToTopicMap(String tableToTopicMap) {
        if (Strings.isNullOrEmpty(tableToTopicMap)){
            throw new IllegalArgumentException("tableToTopicMap cannot empty");
        }
        // test.test:test123;test.test1:test234
        Splitter.on(';')
                .omitEmptyStrings()
                .trimResults()
                .split(tableToTopicMap)
                .forEach(item ->{
                    String[] result =  item.split(":");
                    Preconditions.checkArgument(result.length == 2,
                            "tableToTopicMap format incorrect eg:db.tbl1:topic1;db.tbl2:topic2");
                    filterTableList.add(result[0].trim());
                    this.tableToTopicMap.put(result[0].trim(), result[1].trim());
                });
    }

    public Map<String, String> getTableToTopicMap() {
        return tableToTopicMap;
    }

    /*
    * 根据表名获取 topic
    * */
    public String getTableTopic(String schemaTableName) {
        if (this.tableToTopicMap != null)
            return this.tableToTopicMap.getOrDefault(schemaTableName, CanalSourceConstants.DEFAULT_NOT_MAP_TOPIC);
        else
            return CanalSourceConstants.DEFAULT_NOT_MAP_TOPIC;
    }

    public Table<String, String, Boolean> getTableFieldsFilter() {
        return this.tableFieldsFilter;
    }

    public String getZkServers() {
        return zkServers;
    }

    public void setZkServers(String zkServers) {
        if (Strings.isNullOrEmpty(zkServers)){
            throw new IllegalArgumentException("zkServers cannot empty");
        }
        this.zkServers = zkServers;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        if (Strings.isNullOrEmpty(destination)){
            throw new IllegalArgumentException("destination cannot empty");
        }
        this.destination = destination;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public String getServerUrl() {
        return serverUrl;
    }

    public void setServerUrl(String serverUrl) {
        this.serverUrl = serverUrl;
    }

    public String getServerUrls() {
        return serverUrls;
    }

    public void setServerUrls(String serverUrls) {
        this.serverUrls = serverUrls;
    }

    /*
    * 获取需要过滤的表列表
    * */
    public String getTableFilter() {
        return Joiner.on(",").join(filterTableList);
    }


    public Boolean getOldDataRequired() {
        return oldDataRequired;
    }

    public void setOldDataRequired(Boolean oldDataRequired) {
        this.oldDataRequired = oldDataRequired;
    }

    public boolean isConnectionUrlValid() {
        if (Strings.isNullOrEmpty(this.zkServers)
                && Strings.isNullOrEmpty(this.serverUrl)
                && Strings.isNullOrEmpty(this.serverUrls)) {
            return false;
        } else {
            return true;
        }
    }

    public static List<SocketAddress> convertUrlsToSocketAddressList(String serverUrls) throws ServerUrlsFormatException {
        List<SocketAddress> addresses = new ArrayList<>();

        if (StringUtils.isNotEmpty(serverUrls)) {
            for (String serverUrl : serverUrls.split(",")) {
                if (StringUtils.isNotEmpty(serverUrl)) {
                    try {
                        addresses.add(convertUrlToSocketAddress(serverUrl));
                    } catch (Exception exception) {
                        throw new ServerUrlsFormatException(String.format("The serverUrls are malformed . The ServerUrls : \"%s\" .", serverUrls), exception);
                    }
                }
            }
            return addresses;
        } else {
            return addresses;
        }
    }

    public static SocketAddress convertUrlToSocketAddress(String serverUrl) throws ServerUrlsFormatException, NumberFormatException {
        String[] hostAndPort = serverUrl.split(":");

        if (hostAndPort.length == 2 && StringUtils.isNotEmpty(hostAndPort[1])) {
            try {
                int port  = Integer.parseInt(hostAndPort[1]);
                InetSocketAddress socketAddress = new InetSocketAddress(hostAndPort[0], port);
                return socketAddress;

            } catch (NumberFormatException exception) {
                throw exception;
            }
        } else {
            throw new ServerUrlsFormatException(String.format("The serverUrl is malformed . The ServerUrl : \"%s\" .", serverUrl));
        }
    }
}
