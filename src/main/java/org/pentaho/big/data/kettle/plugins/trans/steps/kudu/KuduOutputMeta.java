package org.pentaho.big.data.kettle.plugins.trans.steps.kudu;

import org.pentaho.di.core.ProvidesModelerMeta;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.shared.SharedObjectInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.pentaho.metastore.api.IMetaStore;

import java.util.List;
import org.w3c.dom.Node;

/**
 * kudu元数据
 */
@Step(id="KuduOutput",image = "KuduOutput.png",
        name = "Kudu表输出",description="KuduOutput.TransDescription",classLoaderGroup="Big Data"
)
public class KuduOutputMeta extends BaseStepMeta implements StepMetaInterface{

    private String hosts;

    private String ports;

    private String tableName;

    private String commitSize;

    @Override
    public void setDefault() {
        hosts = "localhost";
        ports = "7051";
        tableName = "";
        commitSize = "1000";
    }

    @Override
    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
                                 TransMeta transMeta, Trans trans) {
        return new KuduOutput( stepMeta, stepDataInterface, cnr, transMeta, trans );
    }

    @Override
    public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
        readData( stepnode, databases );
    }

    private void readData( Node stepnode, List<? extends SharedObjectInterface> databases ) throws KettleXMLException {
        try {
            tableName = XMLHandler.getTagValue( stepnode, "tableName" );
            commitSize = XMLHandler.getTagValue( stepnode, "commitSize" );
            hosts =  XMLHandler.getTagValue( stepnode, "hosts" );
            ports =  XMLHandler.getTagValue( stepnode, "ports" );
        } catch ( Exception e ) {
            throw new KettleXMLException( "Unable to load step info from XML", e );
        }
    }

    /**
     * 保存文件时需要实现该方法
     * @return
     */
    @Override
    public String getXML() {
        StringBuilder retval = new StringBuilder();
        retval.append( "    " + XMLHandler.addTagValue( "tableName", tableName == null ? "" : tableName) );
        retval.append( "    " + XMLHandler.addTagValue( "commitSize", commitSize == null ? "" : commitSize) );
        retval.append( "    " + XMLHandler.addTagValue( "hosts", hosts == null ? "" : hosts) );
        retval.append( "    " + XMLHandler.addTagValue( "ports", ports == null ? "" : ports) );
        return retval.toString();
    }


    @Override
    public void saveRep(Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step )
            throws KettleException {
        try {
            if (!Utils.isEmpty(tableName)) {
                rep.saveStepAttribute(id_transformation, id_step, "tableName", tableName);
            }
            if (!Utils.isEmpty(commitSize)) {
                rep.saveStepAttribute(id_transformation, id_step,  "commitSize", commitSize);
            }
            if (!Utils.isEmpty(hosts)) {
                rep.saveStepAttribute(id_transformation, id_step, "hosts", hosts);
            }
            if (!Utils.isEmpty(ports)) {
                rep.saveStepAttribute(id_transformation, id_step, "ports", ports);
            }
        }catch (KettleException e){
            throw new KettleException( "Unable to save step information to the repository for id_step=" + id_step, e );
        }
    }



    @Override
    public StepDataInterface getStepData() {
        // return new KuduOutputData();
       return new KuduOutputData(this.tableName,this.hosts,this.ports,Integer.parseInt(this.commitSize));
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

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getCommitSize() {
        return commitSize;
    }

    public void setCommitSize(String commitSize) {
        this.commitSize = commitSize;
    }
}
