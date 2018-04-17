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

public class CanalSourceConstants {

    public static final String IP_INTERFACE = "ipInterface";

    public static final String ZOOKEEPER_SERVERS = "zkServers";
    public static final String SERVER_URL = "serverUrl";
    public static final String SERVER_URLS = "serverUrls";
    public static final String DESTINATION = "destination";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String BATCH_SIZE = "batchSize";

    public static final String TABLE_TO_TOPIC_MAP = "tableToTopicMap";
    public static final String TABLE_FIELDS_FILTER = "tableFieldsFilter";

    public static final String DEFAULT_NOT_MAP_TOPIC = "cannot_map";
    public static final int DEFAULT_BATCH_SIZE = 1024;
    public static final String DEFAULT_USERNAME = "";
    public static final String DEFAULT_PASSWORD = "";

    public static final String SOURCE_TABLES_COUNTER = "SourceTables";


    public static final String HEADER_TOPIC = "topic";
    public static final String HEADER_SCHEMA = "schema";
    public static final String HEADER_KEY = "key";
    public static final String SQL = "sql";


    public static final String META_FIELD_TABLE = "__table";
    public static final String META_FIELD_TS = "__ts";
    public static final String META_FIELD_DB = "__db";
    public static final String META_FIELD_TYPE = "__type";
    public static final String META_FIELD_AGENT = "__agent";
    public static final String META_FIELD_FROM = "__from";




}
