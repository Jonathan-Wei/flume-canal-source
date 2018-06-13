package com.citic.source.canal.core;

import com.alibaba.otter.canal.protocol.CanalEntry;
import org.apache.flume.Event;

public interface DataHandlerInterface {
    Event getDataEvent(CanalEntry.RowData rowData,
        CanalEntry.Header entryHeader,
        CanalEntry.EventType eventType);
}