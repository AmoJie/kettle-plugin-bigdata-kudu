package org.pentaho.big.data.kettle.plugins.trans.steps.kudu;

import org.apache.commons.compress.utils.Lists;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 如果是impala创建的表的话，格式如下impala::db_kudu.my_first_kudu_table
 * impala::schemaName.tableName
 */
public class KuduOutput extends BaseStep implements StepInterface {

    private Object LOCK = new Object();

    private KuduOutputMeta meta;

    private KuduOutputData data;

    private KuduClient kuduClient;

    private KuduSession kuduSession;

    private KuduTable kuduTable;

    private Map<String,String> columnMapping;

    Map<String,ColumnSchema> columnSchemaMap;

    /**
     * This is the base step that forms that basis for all steps. You can derive from this class to implement your own
     * steps.
     *
     * @param stepMeta          The StepMeta object to run.
     * @param stepDataInterface the data object to store temporary data, database connections, caches, result sets,
     *                          hashtables etc.
     * @param copyNr            The copynumber for this step.
     * @param transMeta         The TransInfo of which the step stepMeta is part of.
     * @param trans             The (running) transformation to obtain information shared among the steps.
     */
    public KuduOutput(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }


    /**
     * 处理单行数据
     * @param smi
     * @param sdi
     * @return
     * @throws KettleException
     */
    @Override
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
        meta = (KuduOutputMeta) smi;
        data = (KuduOutputData) sdi;

        // this also waits for a previous step to be finished.
        Object[] r = getRow();
        if ( r != null  && first ) {
            data.inputRowMeta = getInputRowMeta();
            data.outputRowMeta = data.inputRowMeta.clone();
        }
        try {
            Object[] outputRowData = writeToTable( getInputRowMeta(), r );
            if ( outputRowData != null ) {
                // in case we want it go further...
                putRow( data.outputRowMeta, outputRowData );
                incrementLinesOutput();
            }

            if ( checkFeedback( getLinesRead() ) ) {
                if ( log.isBasic() ) {
                    logBasic( "linenr " + getLinesRead() );
                }
            }
        } catch ( KettleException e ) {
            logError( "Because of an error, this step can't continue: ", e );
            setErrors( 1 );
            stopAll();
            setOutputDone(); // signal end to receiver(s)
            closeKuduSource();
            return false;
        }
        return true;
    }

    /**
     * 写入表
     * @param rowMeta
     * @param r
     * @return
     * @throws KettleException
     */
    protected Object[] writeToTable(RowMetaInterface rowMeta, Object[] r ) throws KettleException {
        // Stop: last line or error encountered
        if (r == null) {
            if (log.isDetailed()) {
                logDetailed("Last line inserted: stop");
                closeKuduSource();
            }
            return null;
        }

        Object[] insertRowData = r;
        Object[] outputRowData = r;

        String tableName = data.tableName;

        if (Utils.isEmpty(tableName)) {
            throw new KettleStepException("The tablename is not defined (empty)");
        }

        // 以下代码是插入kudu表的
        // 初始化kuduClient
        initKuduClient(data.hosts,data.ports);
        initSession();
        // 打开表
        openTable(tableName);
        Integer commitCounter = data.commitCounterMap.get(tableName);
        if (commitCounter == null) {
            commitCounter = Integer.valueOf(1);
        } else {
            commitCounter++;
        }
        Insert insert = getInsert(insertRowData);
        try {
            kuduSession.apply(insert);
        } catch (Exception ex) {
            throw new KettleStepException("apply row error", ex);
        }
        data.commitCounterMap.put(tableName, Integer.valueOf(commitCounter.intValue()));
        if ((data.commitSize > 0) && ((commitCounter % data.commitSize) == 0)) {
            try {
                kuduSession.flush();
            } catch (Exception ex) {
                throw new KettleStepException("flush rows error", ex);
            }
            // Clear the batch/commit counter...
            //
            data.commitCounterMap.put(tableName, Integer.valueOf(0));
        }

        data.batchBuffer.add(outputRowData);
        outputRowData = null;
        putRow(data.outputRowMeta, r);
        incrementLinesOutput();
        data.batchBuffer.clear();
        return outputRowData;
    }


    /**
     * 获取Insert
     * @param insertRowData
     * @return
     * @throws KettleException
     */
    private Insert getInsert(Object[] insertRowData) throws KettleException {
        RowMetaInterface rowMeta = data.outputRowMeta;
        if(columnMapping == null){
            columnMapping = new HashMap<>();
            Map<String, String> columnNames = kuduTable.getSchema().getColumns()
                    .stream().collect(Collectors.toMap(columnSchema -> columnSchema.getName().toLowerCase(),columnSchema->columnSchema.getName(),(k1,k2)->k2));
            columnSchemaMap = kuduTable.getSchema().getColumns()
                    .stream().collect(Collectors.toMap(columnSchema -> columnSchema.getName().toLowerCase(),Function.identity(),(k1,k2)->k2));
            for ( int i = 0; i < rowMeta.size(); i++ ) {
                ValueMetaInterface valueMeta = rowMeta.getValueMeta( i );
                String name = valueMeta.getName();
                if(columnNames.get(name.toLowerCase()) != null){
                    columnMapping.put(name,columnNames.get(name.toLowerCase()));
                }
            }
        }
        Insert insert = kuduTable.newInsert();
        PartialRow row = insert.getRow();
        for ( int i = 0; i < rowMeta.size(); i++ ) {
            ValueMetaInterface v = rowMeta.getValueMeta( i );
            Object object = insertRowData[ i ];
            logBasic("name:"+v.getName()+"  value:" + object);
            try {
                if(columnMapping.get(v.getName()) == null){
                    continue;
                }
                if(v.isNull( object )){
                    row.setNull(columnMapping.get(v.getName()));
                    continue;
                }
                ColumnSchema columnSchema = columnSchemaMap.get(v.getName().toLowerCase());
                switch (v.getType()){
                    case ValueMetaInterface.TYPE_STRING:
                        row.addString(columnMapping.get(v.getName()),v.getString(object));
                        break;
                    case ValueMetaInterface.TYPE_INTEGER:
                        if(columnSchema.getType() == Type.INT8){
                            row.addInt(columnMapping.get(v.getName()),v.getInteger(object).intValue());
                        }else if(columnSchema.getType() == Type.INT16
                                || columnSchema.getType() == Type.INT32 || columnSchema.getType() == Type.INT64){
                            row.addLong(columnMapping.get(v.getName()),v.getInteger(object).longValue());
                        }else{
                            row.setNull(columnMapping.get(v.getName()));
                        }
                        break;
                    case ValueMetaInterface.TYPE_NUMBER:
                        if(columnSchema.getType() == Type.INT8){
                            row.addInt(columnMapping.get(v.getName()),v.getInteger(object).intValue());
                        }else if(columnSchema.getType() == Type.INT16
                                || columnSchema.getType() == Type.INT32 || columnSchema.getType() == Type.INT64){
                            row.addLong(columnMapping.get(v.getName()),v.getInteger(object).longValue());
                        }else if(columnSchema.getType() == Type.DECIMAL){
                            row.addDecimal(columnMapping.get(v.getName()),v.getBigNumber(object));
                        }else if(columnSchema.getType() == Type.FLOAT){
                            row.addFloat(columnMapping.get(v.getName()),Float.parseFloat(String.valueOf(v.getNumber(object))));
                        }else if(columnSchema.getType() == Type.DOUBLE){
                            row.addDouble(columnMapping.get(v.getName()),v.getNumber(object));
                        }else{
                            row.setNull(columnMapping.get(v.getName()));
                        }

                    case ValueMetaInterface.TYPE_DATE:
                        row.addTimestamp(columnMapping.get(v.getName()),new Timestamp(v.getDate(object).getTime()));
                    case ValueMetaInterface.TYPE_BINARY:
                        row.addBinary(columnMapping.get(v.getName()),v.getBinary(object));
                    case ValueMetaInterface.TYPE_BOOLEAN:
                        row.addBoolean(columnMapping.get(v.getName()),v.getBoolean(object));
                    case ValueMetaInterface.TYPE_BIGNUMBER:
                        if(columnSchema.getType() == Type.INT8 ){
                            row.addInt(columnMapping.get(v.getName()),v.getInteger(object).intValue());
                        }else if(columnSchema.getType() == Type.INT16
                                || columnSchema.getType() == Type.INT32 || columnSchema.getType() == Type.INT64){
                            row.addLong(columnMapping.get(v.getName()),v.getInteger(object).longValue());
                        }else if(columnSchema.getType() == Type.DECIMAL){
                            row.addDecimal(columnMapping.get(v.getName()),v.getBigNumber(object));
                        }else{
                            row.setNull(columnMapping.get(v.getName()));
                        }
                }
            } catch ( Exception e ) {
                throw new KettleException( "offending row : " + rowMeta, e );
            }
        }
        return insert;
    }


    /**
     * 初始化kudu客户端
     */
    public void initKuduClient(String hostname,String port) throws KettleDatabaseException {
        if(kuduClient == null){
            synchronized (LOCK){
                if(kuduClient == null){
                    String url = "%s:%s";
                    String _hostname = hostname;
                    String _port = port;
                    if (Utils.isEmpty(hostname)) {
                        _hostname = "localhost";
                    }
                    if (Utils.isEmpty(port) || port.equals("-1")) {
                        _port = String.valueOf(7051);
                    }
                    String[] hosts = hostname.split(",");
                    String[] ports = port.split(",");
                    List<String> masters = Lists.newArrayList();
                    if(ports.length == 1){
                        for (String host : hosts) {
                            masters.add(String.format(url, host,_port));
                        }
                    }else if(ports.length == hosts.length){
                        for(int i = 0; i < ports.length; i++){
                            masters.add(String.format(url, hosts[i],ports[i]));
                        }
                    }else{
                        throw new KettleDatabaseException("主机地址和端口号数量不匹配");
                    }
                    String  kuduMaster = String.join(",",masters);
                    kuduClient = new KuduClient.KuduClientBuilder(kuduMaster).build();
                    logBasic("初始化kuduClient成功,kuduMaster="+kuduMaster);
                }
            }
        }
    }

    /**
     * 初始化session
     */
    public void initSession(){
        if(kuduSession == null){
            kuduSession = kuduClient.newSession();
            // 设置批量插入
            kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
            kuduSession.setMutationBufferSpace(10 * 1024 * 1024);
            logBasic("初始化kuduSession成功");
        }
    }

    /**
     * 打开表
     * @param tableName
     * @throws KettleException
     */
    public void openTable(String tableName) throws KettleException {
        if(kuduTable == null){
            try{
                kuduTable = kuduClient.openTable(tableName);
                logBasic("打开kudu表:"+tableName);
            }catch (Exception e){
                throw new KettleException("打开kudu表失败");
            }
        }
    }

    /**
     * 关闭资源
     */
    private void closeKuduSource()  {
        try{
            if(kuduSession != null){
                kuduSession.flush();
                kuduSession.close();
            }
            if(kuduClient != null){
                kuduClient.close();
            }
            logBasic("关闭kudu资源成功");
        }catch (Exception e){
            logError("关闭kudu资源失败",e);
        }
    }


    public KuduOutputMeta getMeta() {
        return meta;
    }

    public void setMeta(KuduOutputMeta meta) {
        this.meta = meta;
    }

    public KuduOutputData getData() {
        return data;
    }

    public void setData(KuduOutputData data) {
        this.data = data;
    }

}
