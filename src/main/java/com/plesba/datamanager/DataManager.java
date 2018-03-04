/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.plesba.datamanager;

import com.plesba.datamanager.source.CSVSource;
import com.plesba.datamanager.target.KinesisTarget;
import com.plesba.datamanager.target.CSVTarget;
import com.plesba.datamanager.target.DBTarget;
import com.plesba.datamanager.source.KinesisSource;
import com.plesba.datamanager.utils.DBConnection;
import com.plesba.datamanager.utils.DMProperties;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.sql.Connection;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
/**
 *
 * @author renee
 */
public class DataManager {
    
        private static String propertiesFile = null;
        private static Properties dataMgrProps = null;
        private static DBConnection dbConnection = null; 
        private static Connection connection = null;
        private static PipedOutputStream outputStream1 = null;
        private static PipedInputStream inputStream1 = null;
        private static CSVSource csvSource = null;
        private static DBTarget dbLoader = null;
        private static CSVTarget csvWriter = null;
        private static KinesisTarget kWriter = null;
        private static KinesisSource kReader = null;
        private static Properties kwProp;
        private static Properties krProp;

    public static void main(String[] args) throws IOException {

        System.out.println("DataManager starting main........");

        if (args.length == 1) {
            propertiesFile = args[0];
            System.out.println("DataManager Properties file: " + propertiesFile);
        } else {
            System.err.println("DataManager <propertiesFile>" + "Usage: java " + DataManager.class.getName());
            System.exit(1);
        }

        dataMgrProps = new DMProperties(propertiesFile).getProp();

        inputStream1 = new PipedInputStream();
        outputStream1 = new PipedOutputStream(inputStream1);


        //pick a source

        //csvreader - read from csv file / write to output stream
        System.out.println("DataManager Selected read from csv file: " + dataMgrProps.getProperty("infilename"));
        csvSource = new CSVSource(dataMgrProps.getProperty("infilename"), outputStream1);
        new Thread(
                new Runnable() {
                    public void run() {
                        csvSource.putDataOnOutputStream();
                    }
                }
        ).start();

        //dbreader



        //kinesis consumer reads from kinesis stream / writes to output stream
     //   System.out.println("Selected read from Kinesis stream/write to output stream: ");

     //   krProp = new Properties();
     //   krProp.setProperty("kinesis.streamname", dataMgrProps.getProperty("kinesis.streamname"));
     //   krProp.setProperty("kinesis.streamsize", dataMgrProps.getProperty("kinesis.streamsize"));
     //   krProp.setProperty("kinesis.region", dataMgrProps.getProperty("kinesis.region"));
     //   krProp.setProperty("kinesis.partitionkey", dataMgrProps.getProperty("kinesis.partitionkey"));
     //   krProp.setProperty("kinesis.endpoint", dataMgrProps.getProperty("kinesis.endpoint"));
     //   krProp.setProperty("kinesis.initialpositioninstream", dataMgrProps.getProperty("kinesis.initialpositioninstream"));
     //   krProp.setProperty("kinesis.applicationname", dataMgrProps.getProperty("kinesis.applicationname"));

     //   try {
     //       kReader = new KinesisSource(krProp, outputStream2);
     //       kReader.processDatafromStream();
     //   } catch (Exception ex) {
     //       Logger.getLogger(KinesisSource.class.getName()).log(Level.SEVERE, null, ex);
     //   }

        // pick a target

        //dbwriter - read from input stream / write to db
        // System.out.println("Selected write to database ");
        //dbConnection = getDBConnection();
        //connection = dbConnection.getConnection();
        //dbLoader = new DBTarget(connection, inputStream);
        //System.out.println("Beginning loading DB");
        //dbLoader.processDataFromInputStream();

        //kinesis producer, read from input stream / write to kinesis stream (producer)
        System.out.println("DataManager Selected write to KinesisTarget stream (producer): ");

        kwProp = new Properties();
        kwProp.setProperty("kinesis.streamname", dataMgrProps.getProperty("kinesis.streamname"));
        kwProp.setProperty("kinesis.streamsize", dataMgrProps.getProperty("kinesis.streamsize"));
        kwProp.setProperty("kinesis.region", dataMgrProps.getProperty("kinesis.region"));
        kwProp.setProperty("kinesis.partitionkey", dataMgrProps.getProperty("kinesis.partitionkey"));

        try {
            kWriter = new KinesisTarget(kwProp, inputStream1);
            kWriter.processDataFromInputStream();
        } catch (InterruptedException ex) {
            Logger.getLogger(KinesisTarget.class.getName()).log(Level.SEVERE, null, ex);
        }

        //csvwriter - read from input stream / write to csv file
        //System.out.println("DataManager selected write to csv file: " + dataMgrProps.getProperty("outfilename"));
        //csvWriter = new CSVTarget(dataMgrProps.getProperty("outfilename"), inputStream1);
        //csvWriter.processDataFromInputStream();

        System.out.println("DataManager Completed................");
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