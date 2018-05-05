package com.citic.source.canal;

import com.alibaba.otter.canal.protocol.CanalEntry;
import org.apache.flume.Event;

interface DataHandlerInterface {
    Event getDataEvent(CanalEntry.RowData rowData,
        CanalEntry.Header entryHeader,
        CanalEntry.EventType eventType,
        String sql);
}