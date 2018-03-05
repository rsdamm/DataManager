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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

    private static final Log LOG = LogFactory.getLog(DataManager.class);

    public static void main(String[] args) throws IOException {

        LOG.info("DataManager starting main........");

        if (args.length == 1) {
            propertiesFile = args[0];
            LOG.info("DataManager Properties file: " + propertiesFile);
        } else {

            LOG.info("DataManager <propertiesFile>" + "Usage: java " + DataManager.class.getName());
            System.exit(1);
        }

        dataMgrProps = new DMProperties(propertiesFile).getProp();

        inputStream1 = new PipedInputStream();
        outputStream1 = new PipedOutputStream(inputStream1);


        //pick a source

        //kinesis consumer, read from kinesis stream / write to output stream
        LOG.info("DataManager Selected write to KinesisSource stream (consumer). ");

        krProp = new Properties();
        krProp.setProperty("kinesis.streamname", dataMgrProps.getProperty("kinesis.streamname"));
        krProp.setProperty("kinesis.streamsize", dataMgrProps.getProperty("kinesis.streamsize"));
        krProp.setProperty("kinesis.region", dataMgrProps.getProperty("kinesis.region"));
        krProp.setProperty("kinesis.partitionkey", dataMgrProps.getProperty("kinesis.partitionkey"));

        try {
            kReader = new KinesisSource(krProp, outputStream1);
            new Thread(
                        new Runnable() {
                            public void run() {
                               kReader.processData();
                            }
                        }
                    ).start();
            } catch (Exception ex) {
            Logger.getLogger(KinesisSource.class.getName()).log(Level.SEVERE, null, ex);
        }

        //csvreader - read from csv file / write to output stream
        //LOG.info("DataManager Selected read from csv file: " + dataMgrProps.getProperty("infilename"));
        //csvSource = new CSVSource(dataMgrProps.getProperty("infilename"), outputStream1);
        //new Thread(
        //        new Runnable() {
        //           public void run() {
        //               csvSource.putDataOnOutputStream();
        //            }
        //       }
        //).start();

        //kinesis producer, read from input stream / write to kinesis stream (producer)
        LOG.info("DataManager Selected write to KinesisTarget stream (producer). ");

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

        LOG.info("DataManager Completed................");
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