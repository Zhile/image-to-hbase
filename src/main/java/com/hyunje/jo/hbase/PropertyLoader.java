package com.hyunje.jo.hbase;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Description
 *
 * @author hyunje
 * @since 14. 11. 14.
 */
public class PropertyLoader {
    static String propertyName = "hbase.properties";

    public Properties getProperties() {
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propertyName);
        Properties properties = new Properties();
        try {
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return properties;
    }
}
