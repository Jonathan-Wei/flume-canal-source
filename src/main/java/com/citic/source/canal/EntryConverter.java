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

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.citic.instrumentation.SourceCounter;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


class EntryConverter {
    private static final Logger LOGGER = LoggerFactory.getLogger(EntryConverter.class);

    static List<Event> convert(CanalEntry.Entry entry, CanalConf canalConf, SourceCounter tableCounter) {
        List<Event> events = new ArrayList<>();
        /*
        * TODO: 事务相关,暂不做处理
        * */
        if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND
                || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN) {

            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                CanalEntry.TransactionEnd end = null;
                try {
                    end = CanalEntry.TransactionEnd.parseFrom(entry.getStoreValue());
                } catch (InvalidProtocolBufferException e) {
                    LOGGER.warn("parse transaction end event has an error , data:" +  entry.toString());
                    throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                }
            }
        }

        if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
            CanalEntry.RowChange rowChange;
            try {
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                LOGGER.warn("parse row data event has an error , data:" + entry.toString(), e);
                throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
            }
            CanalEntry.EventType eventType = rowChange.getEventType();
            CanalEntry.Header eventHeader = entry.getHeader();

            // canal 在 QUERY 事件没有做表过滤
            if (eventType == CanalEntry.EventType.QUERY) {
                // do nothing
            } else if (rowChange.getIsDdl()) {
                // 只有 ddl 操作才记录 sql, 其他 insert update delete 不做sql记录操作
                events.add(EntrySQLHandler.getSqlEvent(eventHeader, rowChange.getSql(), canalConf));
            } else {
                for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                    Event dataEvent = EntryDataHandler.getDataEvent(rowData, eventHeader,
                            eventType, canalConf, tableCounter);
                    events.add(dataEvent);
                }
            }
        }
        return events;
    }
}
