package com.citic.source.canal.core;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.citic.source.canal.CanalConf;
import org.apache.flume.Event;

public interface EntrySqlHandlerInterface {

    Event getSqlEvent(CanalEntry.Header entryHeader, String sql, CanalConf canalConf);
}