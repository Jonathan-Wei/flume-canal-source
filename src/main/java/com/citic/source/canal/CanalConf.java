
package com.citic.source.canal;

import com.citic.helper.RegexHashMap;
import com.citic.helper.Utility;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Canal conf.
 */
public class CanalConf {

    private static final Logger LOGGER = LoggerFactory.getLogger(CanalConf.class);
    private String agentIpAddress;
    private String zkServers;
    private String destination;
    private String username;
    private String password;
    private int batchSize;
    private String serverUrl;
    private String serverUrls;
    private String fromDbIp;

    private boolean shutdownFlowCounter;

    private String tableToTopicMap;
    private String tableFieldsFilter;

    // db.table -> topic
    private Map<String, String> tableToTopicRegexMap = new RegexHashMap<>();
    private List<String> filterTableList = Lists.newArrayList();

    /**
     * Convert urls to socket address list list.
     *
     * @param serverUrls the server urls
     * @return the list
     * @throws IllegalArgumentException the illegal argument exception
     */
    static List<SocketAddress> convertUrlsToSocketAddressList(String serverUrls) throws
        IllegalArgumentException {
        List<SocketAddress> addresses = new ArrayList<>();
        if (StringUtils.isNotEmpty(serverUrls)) {
            for (String serverUrl : serverUrls.split(",")) {
                if (StringUtils.isNotEmpty(serverUrl)) {
                    try {
                        addresses.add(convertUrlToSocketAddress(serverUrl));
                    } catch (Exception exception) {
                        throw new IllegalArgumentException(
                            String.format("The serverUrls are malformed. "
                                + "The ServerUrls : \"%s\" .", serverUrls), exception);
                    }
                }
            }
            return addresses;
        } else {
            return addresses;
        }
    }

    /**
     * Convert url to socket address socket address.
     *
     * @param serverUrl the server url
     * @return the socket address
     * @throws IllegalArgumentException the illegal argument exception
     * @throws NumberFormatException the number format exception
     */
    static SocketAddress convertUrlToSocketAddress(String serverUrl)
        throws IllegalArgumentException,
        NumberFormatException {
        String[] hostAndPort = serverUrl.split(":");
        if (hostAndPort.length == 2 && StringUtils.isNotEmpty(hostAndPort[1])) {
            int port = Integer.parseInt(hostAndPort[1]);
            return new InetSocketAddress(hostAndPort[0], port);
        } else {
            throw new IllegalArgumentException(
                String.format("The serverUrl is malformed . The ServerUrl : \"%s\" .",
                    serverUrl));
        }
    }

    /**
     * Gets table fields filter.
     *
     * @return the table fields filter
     */
    public String getTableFieldsFilter() {
        return this.tableFieldsFilter;
    }

    /**
     * Sets table fields filter.
     *
     * @param tableFieldsFilter the table fields filter
     */
    public void setTableFieldsFilter(String tableFieldsFilter) {
        this.tableFieldsFilter = tableFieldsFilter;
    }

    /**
     * Gets table to topic map.
     *
     * @return the table to topic map
     */
    public String getTableToTopicMap() {
        return this.tableToTopicMap;
    }

    /**
     * Sets table to topic map.
     *
     * @param tableToTopicMap the table to topic map
     */
    public void setTableToTopicMap(String tableToTopicMap) {
        this.tableToTopicMap = tableToTopicMap;
        splitTableToTopicMap(tableToTopicMap);
    }

    /**
     * Gets from db ip.
     *
     * @return the from db ip
     */
    public String getFromDbIp() {
        return this.fromDbIp;
    }

    private void splitTableToTopicMap(String tableToTopicMap) {
        Preconditions
            .checkArgument(!Strings.isNullOrEmpty(tableToTopicMap), "tableToTopicMap cannot empty");
        Splitter.on(';')
            .omitEmptyStrings()
            .trimResults()
            .split(tableToTopicMap)
            .forEach(item -> {
                String[] result = item.split(":");

                Preconditions.checkArgument(result.length >= 2,
                    "tableToTopicMap format incorrect json: db.tbl1:topic1;db.tbl2:topic2 "
                        + "avro: db.tbl1:topic1:schema1;db.tbl2:topic2:schema2");

                Preconditions.checkArgument(!Strings.isNullOrEmpty(result[0].trim()),
                    "db.table cannot empty");
                Preconditions.checkArgument(!Strings.isNullOrEmpty(result[1].trim()),
                    "topic cannot empty");

                filterTableList.add(result[0].trim());
                // db.table -> topic
                this.tableToTopicRegexMap.put(result[0].trim(), result[1].trim());
            });
    }

    /**
     * Gets table topic.
     *
     * @param schemaTableName the schema table name
     * @return the table topic
     */
    public String getTableTopic(String schemaTableName) {
        if (this.tableToTopicRegexMap != null) {
            return this.tableToTopicRegexMap
                .getOrDefault(schemaTableName, CanalSourceConstants.DEFAULT_NOT_MAP_TOPIC);
        } else {
            return CanalSourceConstants.DEFAULT_NOT_MAP_TOPIC;
        }
    }

    /**
     * Is shutdown flow counter boolean.
     *
     * @return the boolean
     */
    public boolean isShutdownFlowCounter() {
        return shutdownFlowCounter;
    }

    /**
     * Sets shutdown flow counter.
     *
     * @param shutdownFlowCounter the shutdown flow counter
     */
    public void setShutdownFlowCounter(boolean shutdownFlowCounter) {
        this.shutdownFlowCounter = shutdownFlowCounter;
    }

    /**
     * Gets agent ip address.
     *
     * @return the agent ip address
     */
    public String getAgentIpAddress() {
        return agentIpAddress;
    }

    /**
     * Gets zk servers.
     *
     * @return the zk servers
     */
    public String getZkServers() {
        return zkServers;
    }

    /**
     * Sets zk servers.
     *
     * @param zkServers the zk servers
     */
    public void setZkServers(String zkServers) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(zkServers), "zkServers cannot empty");
        this.zkServers = zkServers;
    }

    /**
     * Gets destination.
     *
     * @return the destination
     */
    public String getDestination() {
        return destination;
    }

    /**
     * Sets destination.
     *
     * @param destination the destination
     */
    public void setDestination(String destination) {
        Preconditions
            .checkArgument(!Strings.isNullOrEmpty(destination), "destination cannot empty");
        this.destination = destination;
        // destination example: 192_168_2_24-3306
        this.fromDbIp = this.destination.replace("-", ":").replace("_", ".");
    }

    /**
     * Gets username.
     *
     * @return the username
     */
    public String getUsername() {
        return username;
    }

    /**
     * Sets username.
     *
     * @param username the username
     */
    public void setUsername(String username) {
        this.username = username;
    }

    /**
     * Gets password.
     *
     * @return the password
     */
    public String getPassword() {
        return password;
    }

    /**
     * Sets password.
     *
     * @param password the password
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * Gets batch size.
     *
     * @return the batch size
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Sets batch size.
     *
     * @param batchSize the batch size
     */
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    /**
     * Gets server url.
     *
     * @return the server url
     */
    public String getServerUrl() {
        return serverUrl;
    }

    /**
     * Sets server url.
     *
     * @param serverUrl the server url
     */
    public void setServerUrl(String serverUrl) {
        this.serverUrl = serverUrl;
    }

    /**
     * Gets server urls.
     *
     * @return the server urls
     */
    String getServerUrls() {
        return serverUrls;
    }

    /**
     * Sets server urls.
     *
     * @param serverUrls the server urls
     */
    public void setServerUrls(String serverUrls) {
        this.serverUrls = serverUrls;
    }

    /**
     * Sets ip interface.
     *
     * @param ipInterface the ip interface
     */
    public void setIpInterface(String ipInterface) {
        agentIpAddress = Utility.getLocalIp(ipInterface);
    }

    /**
     * Gets filter table list.
     *
     * @return the filter table list
     */
    /*
     * 获取需要过滤的表列表
     * */
    public List<String> getFilterTableList() {
        return filterTableList;
    }

    /**
     * Is connection url valid boolean.
     *
     * @return the boolean
     */
    public boolean isConnectionUrlValid() {
        return !(Strings.isNullOrEmpty(this.zkServers)
            && Strings.isNullOrEmpty(this.serverUrl)
            && Strings.isNullOrEmpty(this.serverUrls));
    }
}
