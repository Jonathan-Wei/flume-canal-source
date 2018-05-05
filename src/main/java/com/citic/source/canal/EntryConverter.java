
package com.citic.source.canal;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.List;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class EntryConverter {

    private static final Logger LOGGER = LoggerFactory.getLogger(EntryConverter.class);

    private final EntrySqlHandlerInterface sqlHandler;
    private final DataHandlerInterface dataHandler;

    private String normalSql;

    EntryConverter(boolean useAvro, CanalConf canalConf) {
        if (useAvro) {
            this.sqlHandler = new AbstractEntrySqlHandler.Avro();
            this.dataHandler = new AbstractDataHandler.Avro(canalConf);
        } else {
            this.sqlHandler = new AbstractEntrySqlHandler.Json();
            this.dataHandler = new AbstractDataHandler.Json(canalConf);
        }

    }

    List<Event> convert(CanalEntry.Entry entry, CanalConf canalConf) {
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
                    LOGGER.warn(
                        "parse transaction end event has an error , data:" + entry.toString());
                    throw new RuntimeException(
                        "parse event has an error , data:" + entry.toString(), e);
                }
            }
        }

        if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
            CanalEntry.RowChange rowChange;
            try {
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                LOGGER.warn("parse row data event has an error , data:" + entry.toString(), e);
                throw new RuntimeException("parse event has an error , data:" + entry.toString(),
                    e);
            }
            CanalEntry.EventType eventType = rowChange.getEventType();
            CanalEntry.Header eventHeader = entry.getHeader();

            // canal 在 QUERY 事件没有做表过滤
            if (eventType == CanalEntry.EventType.QUERY) {
                normalSql = rowChange.getSql();
            } else if (rowChange.getIsDdl()) {
                // 只有 ddl 操作才记录 sql, 其他 insert update delete 不做sql记录操作
                events.add(this.sqlHandler.getSqlEvent(eventHeader, rowChange.getSql(), canalConf));
            } else {
                for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                    Event dataEvent = this.dataHandler
                        .getDataEvent(rowData, eventHeader, eventType, normalSql);
                    events.add(dataEvent);
                }
            }
        }
        return events;
    }
}
