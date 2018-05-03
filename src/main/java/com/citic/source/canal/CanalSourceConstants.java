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

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.text.DecimalFormat;
import java.util.Map;

public class CanalSourceConstants {
    static final String IP_INTERFACE = "ipInterface";

    static final String ZOOKEEPER_SERVERS = "zkServers";
    static final String SERVER_URL = "serverUrl";
    static final String SERVER_URLS = "serverUrls";
    static final String DESTINATION = "destination";
    static final String USERNAME = "username";
    static final String PASSWORD = "password";
    static final String BATCH_SIZE = "batchSize";

    static final String TABLE_TO_TOPIC_MAP = "tableToTopicMap";
    static final String TABLE_FIELDS_FILTER = "tableFieldsFilter";

    static final String USE_AVRO = "useAvro";

    static final String SHUTDOWN_FLOW_COUNTER = "shutdownFlowCounter";
    static final boolean DEFAULT_SHUTDOWN_FLOW_COUNTER = false;

    static final String WRITE_SQL_TO_DATA = "writeSQLToData";
    static final boolean DEFAULT_WRITE_SQL_TO_DATA = false;

    static final String DEFAULT_NOT_MAP_TOPIC = "cannot_map";
    static final int DEFAULT_BATCH_SIZE = 1024;
    static final String DEFAULT_USERNAME = "";
    static final String DEFAULT_PASSWORD = "";


    static final String META_FIELD_TABLE = "__table";
    static final String META_FIELD_TS = "__ts";
    static final String META_FIELD_DB = "__db";
    static final String META_FIELD_TYPE = "__type";
    static final String META_FIELD_AGENT = "__agent";
    static final String META_FIELD_FROM = "__from";
    static final String META_FIELD_SQL = "__sql";


    public final static Gson GSON = new Gson();
    public final static Type TOKEN_TYPE = new TypeToken<Map<String, Object>>(){}.getType();
    public final static DecimalFormat DECIMAL_FORMAT_3 = new DecimalFormat(".###");
}
