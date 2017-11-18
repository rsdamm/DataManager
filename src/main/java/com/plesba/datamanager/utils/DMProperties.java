package com.plesba.datamanager.utils;

import java.util.Properties;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.InputStream;

public class DMProperties {

    //Configuration config = null; 
    String propFile;
    Properties prop; 

    public DMProperties(String pfn) {

        propFile = pfn;
        prop = new Properties(); 

        try {

            prop.load(new FileInputStream(propFile));

        } catch (FileNotFoundException ex) {
            System.out.println("Error: unable to open properties");
            ex.printStackTrace();
        } catch (IOException ex) {
            System.out.println("Error: unable to read properties file");
            ex.printStackTrace();
        }  
      
    }
    public Properties getProp(){
        return prop;
    }

}
