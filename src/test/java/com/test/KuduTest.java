package com.test;

import org.apache.commons.compress.utils.Lists;
import org.apache.kudu.client.*;
import org.pentaho.big.data.kettle.plugins.trans.steps.kudu.KuduOutput;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.util.Utils;

import java.util.List;

public class KuduTest {
    public static void main(String[] args) throws KettleDatabaseException, KuduException {
        KuduClient kuduClient =  initKuduClient( "192.168.10.71", "7051");

        KuduSession kuduSession = kuduClient.newSession();
        // 设置批量插入
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        kuduSession.setMutationBufferSpace(10 * 1024 * 1024);
        ListTablesResponse tablesList = kuduClient.getTablesList();
        for (String s : tablesList.getTablesList()) {
            System.out.println(s);
        }

        KuduTable kuduTable = kuduClient.openTable("impala::db_kudu.my_first_kudu_table");

        Insert insert = kuduTable.newInsert();

    }

    public static KuduClient initKuduClient(String hostname,String port) throws KettleDatabaseException {
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
        if (ports.length == 1) {
            for (String host : hosts) {
                masters.add(String.format(url, host, _port));
            }
        } else if (ports.length == hosts.length) {
            for (int i = 0; i < ports.length; i++) {
                masters.add(String.format(url, hosts[i], ports[i]));
            }
        } else {
            throw new KettleDatabaseException("主机地址和端口号数量不匹配");
        }
        String kuduMaster = String.join(",", masters);
        return new KuduClient.KuduClientBuilder(kuduMaster).build();
    }

}
