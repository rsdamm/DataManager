/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.plesba.datamanager;

import com.plesba.datapiper.source.CSVSourceToStream;
import com.plesba.datapiper.source.DBSourceToStream;
import com.plesba.datapiper.source.KinesisSourceToStream;
import com.plesba.datapiper.source.KafkaSourceToStream;
import com.plesba.datapiper.target.DBTargetFromStream;
import com.plesba.datapiper.target.KinesisTargetFromStream;
import com.plesba.datapiper.target.KafkaTargetFromStream;
import com.plesba.datapiper.transformers.NullTransformer;
import com.plesba.datapiper.transformers.ReverseTransformer;
import com.plesba.datapiper.target.CSVTargetFromStream;
import com.plesba.databeamer.BeamTransformer;
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
        private static CSVSourceToStream csvSourceToStream = null;
        private static NullTransformer nullTransformer = null;
        private static BeamTransformer beamTransformer = null;
        private static ReverseTransformer reverseTransformer = null;
        private static DBTargetFromStream dbLoader = null;
        private static DBSourceToStream dbReader = null;
        private static CSVTargetFromStream csvTargetFromStream = null;
        private static KinesisTargetFromStream kWriter = null;
        private static KinesisSourceToStream kReader = null;
        private static KafkaTargetFromStream kfWriter = null;
        private static KafkaSourceToStream kfReader = null;
        private static Properties kwProp;
        private static Properties krProp;
        private static Properties dbProp;
        private static Properties beamProp;
        private static Properties kfwProp;
        private static Properties kfrProp;
        private static String datasource;
        private static String datatarget;
        private static String transformType = "null";
        private static String csvSourceFilename;
        private static String csvTargetFilename;

    private static final Log LOG = LogFactory.getLog(DataManager.class);

    public static void main(String[] args) throws IOException {

        LOG.info("DataManager starting main........");

        //setup properties

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

        if (transformType.equals("beam")) { // beam requires collections not output/input streams
            LOG.info("DataManager BeamTransformer selected.");
            beamProp = new Properties();
            //get properties
            if (datasource.equals("csv")) {
                csvSourceFilename = dataMgrProps.getProperty("csv.infilename");
                beamProp.setProperty("beam.infilename", dataMgrProps.getProperty("csv.infilename"));
            }
            else {
                LOG.error("DataManager no known source selected. See property: dm.datasource");
            }
            if (datatarget.equals("csv")) {
                csvTargetFilename = dataMgrProps.getProperty("csv.outfilename");
                beamProp.setProperty("beam.outfilename", dataMgrProps.getProperty("csv.outfilename"));
                LOG.info("DataManager output to csv. ");
            }
            else {

                LOG.error("DataManager - no known target selected - see property: dm.datatarget");
            }

            beamTransformer = new BeamTransformer(beamProp);
            beamTransformer.processDataFromInput();
        }
        else { // all processes that interact with output/input streams
            inputStream1 = new PipedInputStream();
            outputStream1 = new PipedOutputStream(inputStream1);

            inputStream2 = new PipedInputStream();
            outputStream2 = new PipedOutputStream(inputStream2);
            if (datasource.equals("kinesisstream")) {

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
                    kReader = new KinesisSourceToStream(krProp, outputStream1);
                    new Thread(
                            new Runnable() {
                                public void run() {
                                    kReader.processData();
                                }
                            }
                    ).start();
                } catch (Exception ex) {
                    Logger.getLogger(KinesisSourceToStream.class.getName()).log(Level.SEVERE, null, ex);
                }
            } else if (datasource.equals("kafkastream")) {

                    //kafka consumer, read from kafka stream / write to output stream
                    LOG.info("DataManager input from Kafka stream (consumer). ");

                    kfrProp = new Properties();
                    kfrProp.setProperty("client.id", dataMgrProps.getProperty("kafka.client.id"));
                    kfrProp.setProperty("acks", dataMgrProps.getProperty("kafka.acks"));
                    kfrProp.setProperty("bootstrap.servers", dataMgrProps.getProperty("kafka.bootstrap.servers"));
                    kfrProp.setProperty("topic", dataMgrProps.getProperty("kafka.topic"));
                    kfrProp.setProperty("topic", dataMgrProps.getProperty("kafka.topic"));
                    kfrProp.setProperty("key.deserializer", dataMgrProps.getProperty("kafka.key.deserializer.class"));
                    kfrProp.setProperty("value.deserializer", dataMgrProps.getProperty("kafka.value.deserializer.class"));

                    try {
                        kfReader = new KafkaSourceToStream(krProp, outputStream1);
                        new Thread(
                                new Runnable() {
                                    public void run() {
                                        kfReader.processData();
                                    }
                                }
                        ).start();
                    } catch (Exception ex) {
                        Logger.getLogger(KafkaSourceToStream.class.getName()).log(Level.SEVERE, null, ex);
                    }
                } else if (datasource.equals("csv")) {
                //csvreader - read from csv file / write to output stream
                csvSourceFilename = dataMgrProps.getProperty("csv.infilename");
                csvSourceToStream = new CSVSourceToStream(csvSourceFilename, outputStream1);
                LOG.info("DataManager input from csv file: " + csvSourceToStream);
                new Thread(
                        new Runnable() {
                            public void run() {
                                csvSourceToStream.putDataOnOutputStream();
                            }
                        }
                ).start();
            } else if (datasource.equals("db")) {
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

                dbReader = new DBSourceToStream(connection, outputStream1);
                dbReader.processDataFromDB();
                dbConnection.closeConnection();

            } else {
                LOG.error("DataManager no known source selected. See property: dm.datasource");
            } //end of source configuration setup

            // begin transformer setup
            if (StringUtils.isEmpty(transformType)) {
                nullTransformer = new NullTransformer(inputStream1, outputStream2);
                LOG.info("DataManager no transformer provided. See property: dm.transformtype");
            } else if (transformType.equals("none")) {
                LOG.info("DataManager Nulltransformer selected.");
                nullTransformer = new NullTransformer(inputStream1, outputStream2);
                nullTransformer.processDataFromInputStream();
            } else if (transformType.equals("reverse")) {
                LOG.info("DataManager ReverseTransformer selected.");
                reverseTransformer = new ReverseTransformer(inputStream1, outputStream2);
                reverseTransformer.processDataFromInputStream();
            } else {
                LOG.info("DataManager no known transformer selected. Defaulting to NullTransformer. See property: dm.transformtype");
                nullTransformer = new NullTransformer(inputStream1, outputStream2);
                nullTransformer.processDataFromInputStream();
            }
            // end of transformer setup

            //begin target setup
            if (datatarget.equals("kinesisstream")) {

                //kinesis producer, read from input stream / write to kinesis stream (producer)
                LOG.info("DataManager output to KinesisTargetFromStream stream (producer). ");

                kwProp = new Properties();
                kwProp.setProperty("kinesis.streamname", dataMgrProps.getProperty("kinesis.streamname"));
                kwProp.setProperty("kinesis.streamsize", dataMgrProps.getProperty("kinesis.streamsize"));
                kwProp.setProperty("kinesis.region", dataMgrProps.getProperty("kinesis.region"));
                kwProp.setProperty("kinesis.partitionkey", dataMgrProps.getProperty("kinesis.partitionkey"));
                kwProp.setProperty("kinesis.maxrecordstoprocess", dataMgrProps.getProperty("kinesis.maxrecordstoprocess"));

                try {
                    kWriter = new KinesisTargetFromStream(kwProp, inputStream2);
                    kWriter.processDataFromInputStream();
                } catch (InterruptedException ex) {
                    Logger.getLogger(KinesisTargetFromStream.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            else if (datatarget.equals("kafkastream")) {
                //kafka producer, read from input stream / write to kafka stream (producer)
                LOG.info("DataManager output to KafkaTargetFromStream stream (producer). ");

                kfwProp = new Properties();
                kfwProp.setProperty("client.id", dataMgrProps.getProperty("kafka.client.id"));
                kfwProp.setProperty("acks", dataMgrProps.getProperty("kafka.acks"));
                kfwProp.setProperty("bootstrap.servers", dataMgrProps.getProperty("kafka.bootstrap.servers"));
                kfwProp.setProperty("topic", dataMgrProps.getProperty("kafka.topic"));
                kfwProp.setProperty("key.serializer", dataMgrProps.getProperty("kafka.key.serializer.class"));
                kfwProp.setProperty("value.serializer", dataMgrProps.getProperty("kafka.value.serializer.class"));
                kfwProp.setProperty("producer.type", dataMgrProps.getProperty("kafka.producer.type"));

                try {
                    kfWriter = new KafkaTargetFromStream(kfwProp, inputStream2);
                    kfWriter.processDataFromInputStream();
                } catch (InterruptedException ex) {
                    Logger.getLogger(KafkaTargetFromStream.class.getName()).log(Level.SEVERE, null, ex);
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

                dbLoader = new DBTargetFromStream(connection, inputStream2);
                dbLoader.processDataFromInputStream();
                dbConnection.closeConnection();

            } else if (datatarget.equals("csv")) {
                //csv, read from input stream / write to  csv

                LOG.info("DataManager output to csv. ");
                csvTargetFilename = dataMgrProps.getProperty("csv.outfilename");
                csvTargetFromStream = new CSVTargetFromStream(csvTargetFilename, inputStream2);
                csvTargetFromStream.processDataFromInputStream();
            } else {

                LOG.error("DataManager - no known target selected - see property: dm.datatarget");
            }
        }
        // end of target setup
        LOG.info("DataManager Completed................");
    }

      
}