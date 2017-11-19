/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.plesba.datamanager;

import com.plesba.datamanager.utils.DBSetup;
import com.plesba.datamanager.utils.DMProperties;
import java.io.IOException;
import java.util.Properties;

/**
 *
 * @author renee
 */
public class DataManager {
    
        private static String propertiesFile = null;
        private static Properties dataMgrProps = null;
        private static DBSetup dbConnection = null;
        
    public static void main(String[] args) throws IOException {

        System.out.println("Starting DataManager main........");

        if (args.length == 1) {
            propertiesFile = args[0];
            System.out.println("Properties file: " + propertiesFile);
        } else {
            System.err.println(" <propertiesFile>" + "Usage: java " + DataManager.class.getName());
            System.exit(1);
        }

        dataMgrProps = new DMProperties(propertiesFile).getProp();
        dbConnection = getConnection(); 
        System.out.println(dbConnection.getConnectString());
        
        System.out.println("Completed DataManager main........");

    }
    public static DBSetup getConnection(){
    
        return new DBSetup.ConnectionBuilder()
                .user(dataMgrProps.getProperty("database.user"))
                .password(dataMgrProps.getProperty("database.password"))
                .database(dataMgrProps.getProperty("database.database"))
                .port(dataMgrProps.getProperty("database.port"))
                .driver(dataMgrProps.getProperty("database.driver"))
                .host(dataMgrProps.getProperty("database.host"))
                .build();
    }   
      
}