# kettle抽取数据到kudu表
### KuduDatabaseMeta 
该类是用来在数据源连接的地方能显示kudu的配置(如果不支持jdbc或者odbc的建议不要这样处理)
### KuduOutputDialog
kudu输出的窗口类，该类比较简单，实现open、ok、cancel方法即可

### KuduOutputMeta
存储元数据的类，窗口中输入的元数据是存储在该类中。
@Step注解中的属性按照该模板进行填写
需要实现getStep、loadXML、readData、getXML、saveRep、getStepData等相关方法。

### KuduOutputData
KuduOutPut类会使用该类

### KuduOutput
数据处理的类
    重点方法为  processRow 与一个构造方法