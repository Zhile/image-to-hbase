package com.hyunje.jo.hbase;

import com.hyunje.jo.hbase.util.PropertyLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.awt.image.WritableRaster;
import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * Description
 *
 * @author hyunje
 * @since 14. 11. 15.
 */
public class SaveImageToHBase {
    public static final String TableName = "Image";
    public static final String imgPath = "testimg.jpg";

    public static void main(String[] args) throws IOException {
        Properties hBaseProp = new PropertyLoader().getProperties();

        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.master", hBaseProp.getProperty("host"));
        configuration.set("hbase.zookeeper.quorum", hBaseProp.getProperty("zooquorum"));
        configuration.set("hbase.zookeeper.property.clientPort",hBaseProp.getProperty("zooport"));


        HTableDescriptor imageTableDescriptor = new HTableDescriptor(org.apache.hadoop.hbase.TableName.valueOf(TableName));
        imageTableDescriptor.addFamily(new HColumnDescriptor("Filename"));
        imageTableDescriptor.addFamily(new HColumnDescriptor("Data"));

        System.out.println("Connecting");
        HBaseAdmin admin = new HBaseAdmin(configuration);
        HTable table = new HTable(configuration, TableName);

        System.out.println("Create table if is not exist");
        createIfNotExist(admin,imageTableDescriptor);

        System.out.println("Loading Image");
        Put image = new Put(Bytes.toBytes("1"));
        image.add(Bytes.toBytes("Filename"), Bytes.toBytes("data"),Bytes.toBytes(imgPath));
        image.add(Bytes.toBytes("Data"),Bytes.toBytes("data"), extractBytes(imgPath));

        System.out.println("Insert Image");
        table.put(image);


    }

    public static void createIfNotExist(HBaseAdmin admin, HTableDescriptor table) throws IOException{
        if(!admin.tableExists(table.getName())){
            admin.createTable(table);
        }
    }

    public static byte[] extractBytes(String imagePath) throws IOException {
        File imageFile = new File(imagePath);
        BufferedImage image = ImageIO.read(imageFile);
        WritableRaster raster = image.getRaster();
        DataBufferByte data = (DataBufferByte) raster.getDataBuffer();
        return data.getData();
    }
}