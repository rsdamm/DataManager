/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.plesba.datamanager;

import com.plesba.datamanager.source.CSVSource;
import com.plesba.datamanager.source.DBSource;
import com.plesba.datamanager.source.KinesisSource;
import com.plesba.datamanager.transformers.NullTransformer;
import com.plesba.datamanager.transformers.ReverseTransformer;
import com.plesba.datamanager.target.KinesisTarget;
import com.plesba.datamanager.target.CSVTarget;
import com.plesba.datamanager.target.DBTarget;
import com.plesba.datamanager.utils.DBConnection;
import com.plesba.datamanager.utils.DMProperties;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.sql.Connection;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang.StringUtils;
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
        private static PipedInputStream inputStream1 = null;
        private static PipedOutputStream outputStream1 = null;
        private static PipedInputStream inputStream2 = null;
        private static PipedOutputStream outputStream2 = null;
        private static CSVSource csvSource = null;
        private static NullTransformer nullTransformer = null;
        private static ReverseTransformer reverseTransformer = null;
        private static DBTarget dbLoader = null;
        private static DBSource dbReader = null;
        private static CSVTarget csvTarget = null;
        private static KinesisTarget kWriter = null;
        private static KinesisSource kReader = null;
        private static Properties kwProp;
        private static Properties krProp;
        private static Properties dbProp;
        private static String datasource;
        private static String datatarget;
        private static String transformType = "null";
        private static String csvSourceFilename;

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

        datasource = dataMgrProps.getProperty("dm.datasource");
        datatarget = dataMgrProps.getProperty("dm.datatarget");
        transformType = dataMgrProps.getProperty("dm.transformtype");

        LOG.info("DataManager datasource =  " + datasource);
        LOG.info("DataManager datatarget =  " + datatarget);
        LOG.info("DataManager transformtype = " + transformType);

        inputStream1 = new PipedInputStream();
        outputStream1 = new PipedOutputStream(inputStream1);

        inputStream2 = new PipedInputStream();       
        outputStream2 = new PipedOutputStream(inputStream2);
        if (datasource.equals( "stream")) {

            //kinesis consumer, read from kinesis stream / write to output stream
            LOG.info("DataManager input from Kinesis stream (consumer). ");

            krProp = new Properties();
            krProp.setProperty("kinesis.streamname", dataMgrProps.getProperty("kinesis.streamname"));
            krProp.setProperty("kinesis.streamsize", dataMgrProps.getProperty("kinesis.streamsize"));
            krProp.setProperty("kinesis.region", dataMgrProps.getProperty("kinesis.region"));
            krProp.setProperty("kinesis.partitionkey", dataMgrProps.getProperty("kinesis.partitionkey"));
            krProp.setProperty("kinesis.initialpositioninstream", dataMgrProps.getProperty("kinesis.initialpositioninstream"));
            krProp.setProperty("kinesis.applicationname", dataMgrProps.getProperty("kinesis.applicationname"));
            krProp.setProperty("kinesis.endpoint", dataMgrProps.getProperty("kinesis.endpoint"));
            krProp.setProperty("kinesis.maxrecordstoprocess", dataMgrProps.getProperty("kinesis.maxrecordstoprocess"));

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
        } else if (datasource.equals( "csv")) {
            //csvreader - read from csv file / write to output stream
            csvSourceFilename = dataMgrProps.getProperty("csv.infilename");
            csvSource = new CSVSource(csvSourceFilename, outputStream1);
            LOG.info("DataManager input from csv file: " + csvSource);
            new Thread(
                    new Runnable() {
                        public void run() {
                            csvSource.putDataOnOutputStream();
                        }
                    }
            ).start();
        } else if (datasource.equals( "db")) {
            //dbsource - read from db / write to output stream
            LOG.info("DataManager input from db: ");
            dbProp = new Properties();
            dbProp.setProperty("database.user", dataMgrProps.getProperty("database.user"));
            dbProp.setProperty("database.password", dataMgrProps.getProperty("database.password"));
            dbProp.setProperty("database.database", dataMgrProps.getProperty("database.database"));
            dbProp.setProperty("database.port", dataMgrProps.getProperty("database.port"));
            dbProp.setProperty("database.driver", dataMgrProps.getProperty("database.driver"));
            dbProp.setProperty("database.host", dataMgrProps.getProperty("database.host"));

            dbConnection = new DBConnection(dbProp);
            connection = dbConnection.getConnection();

            dbReader = new DBSource(connection, outputStream1);
            dbReader.processDataFromDB();
            dbConnection.closeConnection();

        }
        else {
                LOG.error("DataManager no known source selected. See property: dm.datasource" );
        }

        // use a transformer
        if (StringUtils.isEmpty(transformType)) {
            nullTransformer = new NullTransformer(inputStream1, outputStream2);
            LOG.info("DataManager no transformer provided. See property: dm.transformtype");
        } else if (transformType.equals("none")) {
            LOG.info("DataManager Nulltransformer selected.");
            nullTransformer = new NullTransformer(inputStream1, outputStream2);
            nullTransformer.processDataFromInputStream();
        }
        else if (transformType.equals("reverse")) {
                LOG.info("DataManager ReverseTransformer selected.");
                reverseTransformer = new ReverseTransformer(inputStream1, outputStream2);
                reverseTransformer.processDataFromInputStream();
        }
        else {
            LOG.info("DataManager no known transformer selected. Defaulting to NullTranformer. See property: dm.transformtype");
            nullTransformer = new NullTransformer(inputStream1, outputStream2);
            nullTransformer.processDataFromInputStream();
        }

        if (datatarget.equals("stream")) {

            //kinesis producer, read from input stream / write to kinesis stream (producer)
            LOG.info("DataManager output to KinesisTarget stream (producer). ");

            kwProp = new Properties();
            kwProp.setProperty("kinesis.streamname", dataMgrProps.getProperty("kinesis.streamname"));
            kwProp.setProperty("kinesis.streamsize", dataMgrProps.getProperty("kinesis.streamsize"));
            kwProp.setProperty("kinesis.region", dataMgrProps.getProperty("kinesis.region"));
            kwProp.setProperty("kinesis.partitionkey", dataMgrProps.getProperty("kinesis.partitionkey"));
            kwProp.setProperty("kinesis.maxrecordstoprocess", dataMgrProps.getProperty("kinesis.maxrecordstoprocess"));

            try {
                kWriter = new KinesisTarget(kwProp, inputStream2);
                kWriter.processDataFromInputStream();
            } catch (InterruptedException ex) {
                Logger.getLogger(KinesisTarget.class.getName()).log(Level.SEVERE, null, ex);
            }
        } else if (datatarget.equals("db")) {
            //db, read from input stream / write to db
            LOG.info("DataManager output to db. ");

            dbProp = new Properties();
            dbProp.setProperty("database.user", dataMgrProps.getProperty("database.user"));
            dbProp.setProperty("database.password", dataMgrProps.getProperty("database.password"));
            dbProp.setProperty("database.database", dataMgrProps.getProperty("database.database"));
            dbProp.setProperty("database.port", dataMgrProps.getProperty("database.port"));
            dbProp.setProperty("database.driver", dataMgrProps.getProperty("database.driver"));
            dbProp.setProperty("database.host", dataMgrProps.getProperty("database.host"));

            dbConnection = new DBConnection(dbProp);
            connection = dbConnection.getConnection();

            dbLoader = new DBTarget(connection, inputStream2);
            dbLoader.processDataFromInputStream();
            dbConnection.closeConnection();

        } else if (datatarget.equals( "csv")) {
            //csv, read from input stream / write to  csv

            LOG.info("DataManager output to csv. ");
            csvTarget = new CSVTarget(dataMgrProps.getProperty("csv.outfilename"), inputStream2);
            csvTarget.processDataFromInputStream();
        } else {

            LOG.error("DataManager - no known target selected - see property: dm.datatarget");
        }

        LOG.info("DataManager Completed................");
    }

      
}