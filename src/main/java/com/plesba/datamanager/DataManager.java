/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.plesba.datamanager;

import com.plesba.datamanager.source.CSVSource;
import com.plesba.datamanager.utils.DBConnection;
import com.plesba.datamanager.utils.DMProperties;
import com.plesba.datamanager.target.DBWriter;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.sql.Connection;
import java.util.Properties;

/**
 *
 * @author renee
 */
public class DataManager {
    
        private static String propertiesFile = null;
        private static Properties dataMgrProps = null;
        private static DBConnection dbConnection = null; 
        private static Connection connection = null; 
        private static PipedOutputStream outputStream = null;
        private static PipedInputStream inputStream = null;
        private static CSVSource csvReader = null;
        private static DBWriter dbLoader = null; 
        
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
        dbConnection = getDBConnection();
        
        connection = dbConnection.getConnection(); 
        
        inputStream = new PipedInputStream();
        outputStream = new PipedOutputStream(inputStream);
        
        csvReader = new CSVSource(dataMgrProps.getProperty("filename"), outputStream); 
        dbLoader = new DBWriter(connection, inputStream);
        
        new Thread(
                new Runnable() {
            public void run() {
                csvReader.putDataOnOutputStream();
            }
        }
        ).start();
          System.out.println("Beging loading DB=");
        dbLoader.getDataFromInputStream();
        
 
        System.out.println("Completed DataManager main........");

    }
    public static DBConnection getDBConnection(){
    
        return new DBConnection.ConnectionBuilder()
                .user(dataMgrProps.getProperty("database.user"))
                .password(dataMgrProps.getProperty("database.password"))
                .database(dataMgrProps.getProperty("database.database"))
                .port(dataMgrProps.getProperty("database.port"))
                .driver(dataMgrProps.getProperty("database.driver"))
                .host(dataMgrProps.getProperty("database.host")) 
                .build();
    }   
      
}