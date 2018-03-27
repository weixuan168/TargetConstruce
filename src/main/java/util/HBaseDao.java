package util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HBaseDao {

    private static Configuration conf = null;
    private Connection connection = null;
    private static HBaseDao hBaseDao = new HBaseDao();

    private HBaseDao() {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.109.253.156");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.master", "192.168.175.128:60000");
        System.setProperty("hadoop.home.dir", "D:/hadoop-common-2.2.0-bin-master");
    }
    public static HBaseDao getInstance() {
        return hBaseDao;
    }

    public Connection getConnection() {
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return connection;
    }

}
