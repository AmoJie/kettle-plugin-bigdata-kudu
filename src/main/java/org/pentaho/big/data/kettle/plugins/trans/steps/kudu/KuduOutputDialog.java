package org.pentaho.big.data.kettle.plugins.trans.steps.kudu;


import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

/**
 * kudu表输出配置窗口
 *
 */
public class KuduOutputDialog extends BaseStepDialog implements StepDialogInterface {

    private static Class<?> PKG = KuduOutputMeta.class;

    private KuduOutputMeta input;

    private Text wTable,wCommit,wPorts,wHosts;

    private Label wlTable,hosts,ports,wlCommit;

    private FormData fdlTable,fdlHosts,fdlPorts,fdlCommit,fdTable,fdCommit,fdPorts,fdHosts;

    /**
     * Constructor.
     */
    public KuduOutputDialog(Shell parent, Object in, TransMeta transMeta, String sname ) {
        super( parent, (BaseStepMeta) in, transMeta, sname );
        input = (KuduOutputMeta) in;
    }

    @Override
    public String open() {
        Shell parent = getParent();
        Display display = parent.getDisplay();

        shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
        props.setLook( shell );
        setShellImage( shell, input );
        ModifyListener lsMod = new ModifyListener() {
            public void modifyText( ModifyEvent e ) {
                input.setChanged();
            }
        };
        ModifyListener lsTableMod = new ModifyListener() {
            public void modifyText( ModifyEvent arg0 ) {
                input.setChanged();
            }
        };
        backupChanged = input.hasChanged();
        int middle = props.getMiddlePct();
        int margin = Const.MARGIN;

        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = Const.FORM_MARGIN;
        formLayout.marginHeight = Const.FORM_MARGIN;

        shell.setLayout( formLayout );
        shell.setText("kudu表输出");

        // Stepname line
        wlStepname = new Label( shell, SWT.RIGHT );
        wlStepname.setText( BaseMessages.getString( PKG, "System.Label.StepName" ) );
        props.setLook( wlStepname );
        fdlStepname = new FormData();
        fdlStepname.left = new FormAttachment( 0, 0 );
        fdlStepname.right = new FormAttachment( middle, -margin );
        fdlStepname.top = new FormAttachment( 0, margin );
        wlStepname.setLayoutData( fdlStepname );
        wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        wStepname.setText( stepname );
        props.setLook( wStepname );
        wStepname.addModifyListener( lsMod );
        fdStepname = new FormData();
        fdStepname.left = new FormAttachment( middle, 0 );
        fdStepname.top = new FormAttachment( 0, margin );
        fdStepname.right = new FormAttachment( 100, 0 );
        wStepname.setLayoutData( fdStepname );

        // hosts
        hosts = new Label( shell, SWT.RIGHT );
        hosts.setText( "主机名" );
        props.setLook( hosts );
        fdlHosts = new FormData();
        fdlHosts.left = new FormAttachment( 0, 0 );
        fdlHosts.right = new FormAttachment( middle, -margin );
        fdlHosts.top = new FormAttachment( wlStepname, margin + 5);
        hosts.setLayoutData( fdlHosts );

        wHosts = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wHosts );
        wHosts.addModifyListener( lsMod );
        fdHosts = new FormData();
        fdHosts.left = new FormAttachment( middle, 0 );
        fdHosts.top = new FormAttachment( wlStepname, margin + 5 );
        fdHosts.right = new FormAttachment( 100, 0 );
        wHosts.setLayoutData( fdHosts );


        // ports
        ports = new Label( shell, SWT.RIGHT );
        ports.setText( "端口号" );
        props.setLook( ports );
        fdlPorts = new FormData();
        fdlPorts.left = new FormAttachment( 0, 0 );
        fdlPorts.right = new FormAttachment( middle, -margin );
        fdlPorts.top = new FormAttachment( hosts, margin + 5 );
        ports.setLayoutData( fdlPorts );

        wPorts = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wPorts );
        wPorts.addModifyListener( lsMod );
        fdPorts = new FormData();
        fdPorts.left = new FormAttachment( middle, 0 );
        fdPorts.top = new FormAttachment( hosts, margin + 5 );
        fdPorts.right = new FormAttachment( 100, 0 );
        wPorts.setLayoutData( fdPorts );

        // Table line...
        wlTable = new Label( shell, SWT.RIGHT );
        wlTable.setText( "目标表" );
        props.setLook( wlTable );
        fdlTable = new FormData();
        fdlTable.left = new FormAttachment( 0, 0 );
        fdlTable.right = new FormAttachment( middle, -margin );
        fdlTable.top = new FormAttachment( ports, margin + 5 );
        wlTable.setLayoutData( fdlTable );
        wTable = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wTable );
        wTable.addModifyListener( lsMod );
        fdTable = new FormData();
        fdTable.left = new FormAttachment( middle, 0 );
        fdTable.top = new FormAttachment( ports, margin + 5 );
        fdTable.right = new FormAttachment( 100, 0 );
        wTable.setLayoutData( fdTable );


        // Commit size ...
        wlCommit = new Label( shell, SWT.RIGHT );
        wlCommit.setText( "提交记录数量" );
        props.setLook( wlCommit );
        fdlCommit = new FormData();
        fdlCommit.left = new FormAttachment( 0, 0 );
        fdlCommit.right = new FormAttachment( middle, -margin );
        fdlCommit.top = new FormAttachment( wlTable, margin + 5 );
        wlCommit.setLayoutData( fdlCommit );
        wCommit = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wCommit );
        wCommit.addModifyListener( lsMod );
        fdCommit = new FormData();
        fdCommit.left = new FormAttachment( middle, 0 );
        fdCommit.top = new FormAttachment( wlTable, margin + 5 );
        fdCommit.right = new FormAttachment( 100, 0 );
        wCommit.setLayoutData( fdCommit );

        // Some buttons
        wOK = new Button( shell, SWT.PUSH );
        wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );

        wCancel = new Button( shell, SWT.PUSH );
        wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

        setButtonPositions( new Button[] { wOK, wCancel }, margin, null );

        // Add listeners
        lsOK = new Listener() {
            public void handleEvent( Event e ) {
                ok();
            }
        };

        lsCancel = new Listener() {
            public void handleEvent( Event e ) {
                cancel();
            }
        };
        wOK.addListener( SWT.Selection, lsOK );
        wCancel.addListener( SWT.Selection, lsCancel );

        input.setChanged( backupChanged );

        setSize();

        getData();

        shell.open();
        while ( !shell.isDisposed() ) {
            if ( !display.readAndDispatch() ) {
                display.sleep();
            }
        }
        return stepname;
    }


    public void getData() {
        if ( input.getHosts() != null ) {
            wHosts.setText( input.getHosts() );
        }
        if ( input.getPorts() != null ) {
            wPorts.setText( input.getPorts() );
        }
        if ( input.getTableName() != null ) {
            wTable.setText( input.getTableName() );
        }

        if ( input.getCommitSize() != null ) {
            wCommit.setText( input.getCommitSize() );
        }
        wStepname.selectAll();
        wStepname.setFocus();
    }


    private void ok() {
        if ( Utils.isEmpty( wStepname.getText() ) ) {
            logBasic("步骤名称："+wStepname.getText());
            return;
        }
        // return value
        stepname = wStepname.getText();

        getInfo( input );

        dispose();
    }




    private void getInfo( KuduOutputMeta info ) {
        info.setTableName( wTable.getText() );
        info.setCommitSize( wCommit.getText() );
        info.setHosts(wHosts.getText());
        info.setPorts(wPorts.getText());
    }

    private void cancel() {
        stepname = null;
        input.setChanged( backupChanged );
        dispose();
    }
}
