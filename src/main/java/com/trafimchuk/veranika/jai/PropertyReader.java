package com.trafimchuk.veranika.jai;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

public class PropertyReader {

    private String filePath = null;
    private Properties properties = null;

    public PropertyReader(String filePath) {
        this.filePath = filePath;

        properties = new Properties();
        InputStream in = null;
        try {
            in = new FileInputStream(filePath);
            properties.load(in);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public String read(String key) {
        return properties.getProperty(key);
    }

    public void write(String key, String value) {
        properties.setProperty(key, value);

        OutputStream output = null;

        try {
            output = new FileOutputStream(filePath);

            properties.setProperty(key, value);
            properties.store(output, null);

        } catch (IOException ioe) {
            ioe.printStackTrace();
        } finally {
            if (output != null) {
                try {
                    output.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
    }
}
