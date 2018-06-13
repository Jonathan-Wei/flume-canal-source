package com.citic.source.canal.core;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.citic.source.canal.CanalConf;
import java.util.List;
import org.apache.flume.Event;

public interface EntryConverterInterface {

    List<Event> convert(CanalEntry.Entry entry, CanalConf canalConf);
}
