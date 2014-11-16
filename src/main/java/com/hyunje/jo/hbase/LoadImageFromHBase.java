package com.hyunje.jo.hbase;

import com.hyunje.jo.hbase.util.PropertyLoader;
import com.sun.org.apache.xml.internal.security.exceptions.Base64DecodingException;
import com.sun.org.apache.xml.internal.security.utils.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * Description
 *
 * @author hyunje
 * @since 14. 11. 16.
 */
public class LoadImageFromHBase {
    public static final String TableName = "Image";
    public static final String imgPath = "result.jpg";
    public static final String FileNameColumn = "Filename";
    public static final String DataColumn = "Data";
    public static final String DataQualifier = "data";

    public static void main(String[] args) throws IOException, Base64DecodingException {
        Properties hBaseProp = new PropertyLoader().getProperties();

        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.master", hBaseProp.getProperty("host"));
        configuration.set("hbase.zookeeper.quorum", hBaseProp.getProperty("zooquorum"));
        configuration.set("hbase.zookeeper.property.clientPort",hBaseProp.getProperty("zooport"));

        HTableDescriptor imageTableDescriptor = new HTableDescriptor(org.apache.hadoop.hbase.TableName.valueOf(TableName));
        imageTableDescriptor.addFamily(new HColumnDescriptor(FileNameColumn));
        imageTableDescriptor.addFamily(new HColumnDescriptor(DataColumn));

        System.out.println("Connecting");
        HBaseAdmin admin = new HBaseAdmin(configuration);
        HTable table = new HTable(configuration, TableName);

        if (!admin.tableExists(table.getName())){
            System.err.println("Table is not exist");
            System.exit(-1);
        }

        Get get = new Get(Bytes.toBytes("1"));

        Result result = table.get(get);
        if(result.isEmpty()){
            System.exit(-1);
        }

        byte[] imageBytes = result.getValue(Bytes.toBytes(DataColumn), Bytes.toBytes(DataQualifier));

        System.out.println("File length : "+imageBytes.length);
        System.out.println("Saving Image File : "+imgPath);


        BufferedImage img = ImageIO.read(new ByteArrayInputStream(imageBytes));
        ImageIO.write(img,"jpg",new File(imgPath));

    }
}
