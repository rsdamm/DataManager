
package DataManagerUnitTests;

import com.plesba.datamanager.DataManager;
import com.plesba.datapiper.source.CSVSourceToStream;
import com.plesba.datapiper.source.KafkaSourceToStream;
import com.plesba.datapiper.target.CSVTargetFromStream;
import com.plesba.datapiper.target.KafkaTargetFromStream;
import com.plesba.datamanager.utils.DMProperties;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author REnee
 */
public class KafkaSourceToStreamTest {
    private static PipedOutputStream outputStream1 = null;
    private static PipedInputStream inputStream1 = null;
    private static PipedOutputStream outputStream2 = null;
    private static PipedInputStream inputStream2 = null;
    private static String propertiesFile = "/Users/renee/IdeaProjects/DataManager/config.properties";
    private static CSVSourceToStream csvSource = null;
    private static CSVTargetFromStream csvTargetFromStream = null;
    private long recordCountCSVIn = 0;
    private long recordCountCSVOut = 0;
    private long maxRecordsToProcess =3;
    private static KafkaTargetFromStream kWriter = null;
    private static KafkaSourceToStream kReader = null;
    private static final Log LOG = LogFactory.getLog(DataManager.class);
    private static String csvFilenameIn;
    private static String csvFilenameOut;
    private static Properties dataMgrProps = null;
    private static Properties kfwProp;
    private static Properties kfrProp;
    public KafkaSourceToStreamTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of run method, of class DBloader.
     * @throws java.io.IOException
     */
    @Test
    public void testRun() throws IOException {


        LOG.info("KafkaSourceToStreamTest starting");

        dataMgrProps = new DMProperties(propertiesFile).getProp();
        LOG.info("KafkaSourceToStreamTest properties obtained");

        inputStream1 = new PipedInputStream();
        outputStream1 = new PipedOutputStream(inputStream1);
        inputStream2 = new PipedInputStream();
        outputStream2 = new PipedOutputStream(inputStream2);

        csvFilenameIn = dataMgrProps.getProperty("csv.infilename");

        csvSource = new CSVSourceToStream(csvFilenameIn, outputStream1);
        LOG.info("CSVSourceToStream starting CSVSourceToStream: " + csvFilenameIn);

        new Thread(
                new Runnable() {
                    public void run() {
                        csvSource.putDataOnOutputStream();
                    }
                }
        ).start();

        recordCountCSVIn = Files.lines(Paths.get(csvFilenameIn)).count();

        //kafka producer, read from input stream / write to kafka stream (producer)
        LOG.info("KafkaTargetFromStreamTest starting Kafka Target (producer). ");
        kfwProp = new Properties();
        kfwProp.setProperty("client.id", dataMgrProps.getProperty("kafka.client.id"));
        kfwProp.setProperty("acks", dataMgrProps.getProperty("kafka.acks"));
        kfwProp.setProperty("bootstrap.servers", dataMgrProps.getProperty("kafka.bootstrap.servers"));
        kfwProp.setProperty("topic", dataMgrProps.getProperty("kafka.topic"));
        kfwProp.setProperty("key.serializer", dataMgrProps.getProperty("kafka.key.serializer.class"));
        kfwProp.setProperty("value.serializer", dataMgrProps.getProperty("kafka.value.serializer.class"));
        kfwProp.setProperty("producer.type", dataMgrProps.getProperty("kafka.producer.type"));
        kfwProp.setProperty("maxrecordstoprocess", dataMgrProps.getProperty("kafka.maxrecordstoprocess"));

        try {
            kWriter = new KafkaTargetFromStream(kfwProp, inputStream1);
            new Thread(
                    new Runnable() {
                        public void run() {
                            kWriter.processDataFromInputStream();
                        }
                    }
            ).start();
        } catch (InterruptedException ex) {
            Logger.getLogger(KafkaSourceToStream.class.getName()).log(Level.SEVERE, null, ex);
        }

        //Kafka consumer, read from kafka stream / write to output stream
        LOG.info("KafkaSourceToStreamTest starting KafkaSourceToStream (consumer). ");

        kfrProp = new Properties();
        kfrProp.setProperty("client.id", dataMgrProps.getProperty("kafka.client.id"));
        kfrProp.setProperty("acks", dataMgrProps.getProperty("kafka.acks"));
        kfrProp.setProperty("bootstrap.servers", dataMgrProps.getProperty("kafka.bootstrap.servers"));
        kfrProp.setProperty("topic", dataMgrProps.getProperty("kafka.topic"));
        kfrProp.setProperty("key.deserializer", dataMgrProps.getProperty("kafka.key.deserializer.class"));
        kfrProp.setProperty("value.deserializer", dataMgrProps.getProperty("kafka.value.deserializer.class"));
        kfrProp.setProperty("group.id", dataMgrProps.getProperty("kafka.group_id_config"));
        kfrProp.setProperty("maxrecordstoprocess", dataMgrProps.getProperty("kafka.maxrecordstoprocess"));
        //kfrProp.setProperty("maxrecordstoprocess", String.valueOf(maxRecordsToProcess));

        try {
            kReader = new KafkaSourceToStream(kfrProp, outputStream2);
            new Thread(
                    new Runnable() {
                        public void run() {
                            kReader.processData();
                        }
                    }
            ).start();
        } catch (Exception ex) {

            Logger.getLogger(KafkaSourceToStream.class.getName()).log(Level.SEVERE, null, ex);
        }
        //csv, read from input stream / write to  csv
        LOG.info("KafkaSourceToStreamTest starting csv target. ");
        csvFilenameOut=dataMgrProps.getProperty("csv.outfilename");
        csvTargetFromStream = new CSVTargetFromStream(csvFilenameOut, inputStream2);
        csvTargetFromStream.processDataFromInputStream();

        LOG.info("KafkaSourceToStreamTest csv target completed. ");

        recordCountCSVOut = Files.lines(Paths.get(csvFilenameOut)).count();

        assertEquals(maxRecordsToProcess, recordCountCSVOut);
        LOG.info("KafkaSourceToStreamTest completed");
    }

}