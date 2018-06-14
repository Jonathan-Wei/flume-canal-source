
package com.citic.source.canal.resolve;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.citic.source.canal.AbstractEntrySqlHandler.Avro;
import com.citic.source.canal.AbstractEntrySqlHandler.Json;
import com.citic.source.canal.CanalConf;
import com.citic.source.canal.core.DataHandlerInterface;
import com.citic.source.canal.core.EntryConverterInterface;
import com.citic.source.canal.core.EntrySqlHandlerInterface;
import java.util.ArrayList;
import java.util.List;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The type Entry converter.
 */
public class EntryConverter implements EntryConverterInterface {

    private static final Logger LOGGER = LoggerFactory.getLogger(EntryConverter.class);

    private final EntrySqlHandlerInterface sqlHandler;
    private final DataHandlerInterface dataHandler;

    /**
     * Instantiates a new Entry converter.
     *
     * @param useAvro the use avro
     * @param canalConf the canal conf
     */
    public EntryConverter(boolean useAvro, CanalConf canalConf) {
        if (useAvro) {
            this.sqlHandler = new Avro();
            this.dataHandler = new AbstractDataHandler.Avro(canalConf);
        } else {
            this.sqlHandler = new Json();
            this.dataHandler = new AbstractDataHandler.Json(canalConf);
        }
    }

    /**
     * convert entry data to event.
     *
     * @param entry canal data entry
     * @param canalConf the canal conf
     */
    public List<Event> convert(CanalEntry.Entry entry, CanalConf canalConf) {
        List<Event> events = new ArrayList<>();

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
                // do nothing
            } else if (rowChange.getIsDdl()) {
                // 只有 ddl 操作才记录 sql, 其他 insert update delete 不做sql记录操作
                events.add(this.sqlHandler.getSqlEvent(eventHeader, rowChange.getSql(), canalConf));
            } else {
                for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                    Event dataEvent = this.dataHandler
                        .getDataEvent(rowData, eventHeader, eventType);

                    if (dataEvent != null) {
                        events.add(dataEvent);
                    }
                }
            }
        }
        return events;
    }
}
