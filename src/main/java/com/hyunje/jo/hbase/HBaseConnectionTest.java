package com.hyunje.jo.hbase;

import com.hyunje.jo.hbase.util.PropertyLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Properties;

/**
 * HBase에 연결 테스트 클래스.
 *
 * @author hyunje
 * @since 14. 11. 13.
 */
public class HBaseConnectionTest {
    public static void main(String[] args) throws IOException {

        Properties hBaseProp = new PropertyLoader().getProperties();

        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.master", hBaseProp.getProperty("host"));
        configuration.set("hbase.zookeeper.quorum", hBaseProp.getProperty("zooquorum"));
        configuration.set("hbase.zookeeper.property.clientPort",hBaseProp.getProperty("zooport"));

        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("User"));
        tableDescriptor.addFamily(new HColumnDescriptor("Id"));
        tableDescriptor.addFamily(new HColumnDescriptor("Name"));

        System.out.println("Connecting");
        HBaseAdmin admin = new HBaseAdmin(configuration);

        System.out.println("Creating Table");
        createOrOverwrite(admin,tableDescriptor);

        System.out.println("Inserting Data");
        HTable table = new HTable(configuration,"User");

        Put put = new Put(Bytes.toBytes("row1"));
        put.add(Bytes.toBytes("Id"), Bytes.toBytes("col1"), Bytes.toBytes("Emp1"));
        put.add(Bytes.toBytes("Name"), Bytes.toBytes("col2"), Bytes.toBytes("Archana"));

        table.put(put);
        System.out.println("Inserting Completed");
        System.out.println("Retrieve Search Result");

        Get get = new Get(Bytes.toBytes("row1"));

        Result result = table.get(get);

        byte[] value1 = result.getValue(Bytes.toBytes("Id"), Bytes.toBytes("col1"));
        byte[] value2 = result.getValue(Bytes.toBytes("Name"), Bytes.toBytes("col2"));

        String value1String = new String(value1);
        String value2String = new String(value2);

        System.out.println("Get [ Id : "+value1String+", Name : "+value2String+" ]");

        admin.close();

        System.out.println("Done");
    }
    public static void createOrOverwrite(HBaseAdmin admin, HTableDescriptor table) throws IOException {
        if (admin.tableExists(table.getName())) {
            admin.disableTable(table.getName());
            admin.deleteTable(table.getName());
        }
        admin.createTable(table);
    }
}
