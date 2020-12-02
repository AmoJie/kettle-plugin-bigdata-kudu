package org.pentaho.big.data.kettle.plugins.trans.steps.kudu;

import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

import java.sql.PreparedStatement;
import java.sql.Savepoint;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * kudu数据输出
 */
public class KuduOutputData  extends BaseStepData implements StepDataInterface {
    public Database db;
    public int warnings;
    public String tableName;
    // Stream valuename nrs to prevent searches.
    public int[] valuenrs;


    /** Use batch mode or not? */
    public boolean batchMode;

    public String hosts;

    public String ports;

    public List<Object[]> batchBuffer;
    public RowMetaInterface outputRowMeta;
    public RowMetaInterface inputRowMeta;

    public Map<String, Integer> commitCounterMap;

    public int commitSize;

    public KuduOutputData() {
        super();
        db = null;
        warnings = 0;
        tableName = null;
        batchBuffer = new ArrayList<Object[]>();
        commitCounterMap = new HashMap<String, Integer>();
    }

    public KuduOutputData(String tableName,String hosts,String ports,int commitSize ){
       super();
       this.commitSize = commitSize;
       this.hosts = hosts;
       this.tableName = tableName;
       this.ports = ports;
        db = null;
        warnings = 0;
        batchBuffer = new ArrayList<Object[]>();
        commitCounterMap = new HashMap<String, Integer>();
    }

    public Database getDb() {
        return db;
    }

    public void setDb(Database db) {
        this.db = db;
    }

    public int getWarnings() {
        return warnings;
    }

    public void setWarnings(int warnings) {
        this.warnings = warnings;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int[] getValuenrs() {
        return valuenrs;
    }

    public void setValuenrs(int[] valuenrs) {
        this.valuenrs = valuenrs;
    }

    public boolean isBatchMode() {
        return batchMode;
    }

    public void setBatchMode(boolean batchMode) {
        this.batchMode = batchMode;
    }

    public String getHosts() {
        return hosts;
    }

    public void setHosts(String hosts) {
        this.hosts = hosts;
    }

    public String getPorts() {
        return ports;
    }

    public void setPorts(String ports) {
        this.ports = ports;
    }

    public List<Object[]> getBatchBuffer() {
        return batchBuffer;
    }

    public void setBatchBuffer(List<Object[]> batchBuffer) {
        this.batchBuffer = batchBuffer;
    }

    public RowMetaInterface getOutputRowMeta() {
        return outputRowMeta;
    }

    public void setOutputRowMeta(RowMetaInterface outputRowMeta) {
        this.outputRowMeta = outputRowMeta;
    }

    public RowMetaInterface getInputRowMeta() {
        return inputRowMeta;
    }

    public void setInputRowMeta(RowMetaInterface inputRowMeta) {
        this.inputRowMeta = inputRowMeta;
    }

    public Map<String, Integer> getCommitCounterMap() {
        return commitCounterMap;
    }

    public void setCommitCounterMap(Map<String, Integer> commitCounterMap) {
        this.commitCounterMap = commitCounterMap;
    }

    public int getCommitSize() {
        return commitSize;
    }

    public void setCommitSize(int commitSize) {
        this.commitSize = commitSize;
    }
}
