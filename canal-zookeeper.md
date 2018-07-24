
# Canal Zookeeper 路径说明
* 查看当前Canal Server的所有Instance
```
ls /otter/canal/destinations
[192_168_2_26-3306]
```

* 192_168_2_26-3306 Instance（实例）
```
ls /otter/canal/destinations/192_168_2_26-3306
[running, cluster, 1001]
```

* 包含当前 Instance 正在运行的Canal Server信息
```
get /otter/canal/destinations/192_168_2_26-3306/running
{"active":true,"address":"192.168.2.24:11111","cid":1}
cZxid = 0x1147fe
ctime = Mon Jul 23 14:34:23 CST 2018
mZxid = 0x1147fe
mtime = Mon Jul 23 14:34:23 CST 2018
pZxid = 0x1147fe
cversion = 0
dataVersion = 0
aclVersion = 0
ephemeralOwner = 0x164c5d5533d0007
dataLength = 54
numChildren = 0
```
* 包含当前 Instance 所有已经启动Canal Server信息，包括备份Canal Server
```
ls /otter/canal/destinations/192_168_2_26-3306/cluster
[192.168.2.24:11111]
```

* Canal Instance 下当前消费client
```
ls /otter/canal/destinations/192_168_2_26-3306/1001
[filter, cursor, running]
```

* Instance 正在运行的消费端client的信息
```
get /otter/canal/destinations/192_168_2_26-3306/1001/running
{"active":true,"address":"192.168.2.24:46748","clientId":1001}
cZxid = 0x1148d9
ctime = Mon Jul 23 16:37:07 CST 2018
mZxid = 0x1148da
mtime = Mon Jul 23 16:37:07 CST 2018
pZxid = 0x1148d9
cversion = 0
dataVersion = 1
aclVersion = 0
ephemeralOwner = 0x164c5d5533d0020
dataLength = 62
numChildren = 0
```
* Instance 下消费端client 对库和表的消费过滤信息
```
get /otter/canal/destinations/192_168_2_26-3306/1001/filter
test.test5
cZxid = 0x114805
ctime = Mon Jul 23 14:34:44 CST 2018
mZxid = 0x1148dd
mtime = Mon Jul 23 16:37:07 CST 2018
pZxid = 0x114805
cversion = 0
dataVersion = 10
aclVersion = 0
ephemeralOwner = 0x0
dataLength = 10
numChildren = 0
```
* Instance 下消费端client 对当前MySQL实例消费位点信息的记录
如果希望重新消费可以删除这个zookeeper路径
这里记录了MySQL的连接信息和当前消费的binlog名称和位置信息
```
get /otter/canal/destinations/192_168_2_26-3306/1001/cursor
{"@type":"com.alibaba.otter.canal.protocol.position.LogPosition","identity":{"slaveId":-1,"sourceAddress":{"address":"192.168.2.26","port":3306}},"postion":{"included":false,"journalName":"mysql-bin.000042","position":84690,"serverId":1,"timestamp":1532413586000}}
cZxid = 0x114811
ctime = Mon Jul 23 14:34:51 CST 2018
mZxid = 0x114907
mtime = Tue Jul 24 14:26:26 CST 2018
pZxid = 0x114811
cversion = 0
dataVersion = 37
aclVersion = 0
ephemeralOwner = 0x0
dataLength = 264
numChildren = 0
```



