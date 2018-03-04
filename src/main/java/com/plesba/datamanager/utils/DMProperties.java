package com.plesba.datamanager.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Properties;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.InputStream;

public class DMProperties {

    //Configuration config = null; 
    String propFile;
    Properties prop;

    private static final Log LOG = LogFactory.getLog(DMProperties.class);

    public DMProperties(String pfn) {

        propFile = pfn;
        prop = new Properties(); 

        try {

            prop.load(new FileInputStream(propFile));

        } catch (FileNotFoundException ex) {
            LOG.info("DMProperties Error: unable to open properties");
            ex.printStackTrace();
        } catch (IOException ex) {
            LOG.info("DMProperties Error: unable to read properties file");
            ex.printStackTrace();
        }  
      
    }
    public Properties getProp(){
        return prop;
    }
     

}
