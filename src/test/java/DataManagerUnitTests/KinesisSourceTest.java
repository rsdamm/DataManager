package DataManagerUnitTests;

import com.plesba.datamanager.DataManager;
import com.plesba.datamanager.source.CSVSource;
import com.plesba.datamanager.source.KinesisSource;
import com.plesba.datamanager.target.CSVTarget;
import com.plesba.datamanager.target.KinesisTarget;
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
public class KinesisSourceTest {
    private static PipedOutputStream outputStream1 = null;
    private static PipedInputStream inputStream1 = null;
    private static PipedOutputStream outputStream2 = null;
    private static PipedInputStream inputStream2 = null;
    private static String propertiesFile = "/Users/renee/IdeaProjects/DataManager/config.properties";
    private static CSVSource csvSource = null;
    private static CSVTarget csvTarget = null;
    private long recordCountCSVIn = 0;
    private long recordCountCSVOut = 0;
    private long maxRecordsToProcess =3;
    private long maxrecordstoprocess = 0;
    private static KinesisTarget kWriter = null;
    private static KinesisSource kReader = null;
    private static final Log LOG = LogFactory.getLog(DataManager.class);
    private static String csvFilenameIn;
    private static String csvFilenameOut;
    private static Properties dataMgrProps = null;
    private static Properties kwProp;
    private static Properties krProp;
    public KinesisSourceTest() {
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


        LOG.info("KinesisSourceTest starting");

        dataMgrProps = new DMProperties(propertiesFile).getProp();
        LOG.info("KinesisSourceTest properties obtained");

        inputStream1 = new PipedInputStream();
        outputStream1 = new PipedOutputStream(inputStream1);
        inputStream2 = new PipedInputStream();
        outputStream2 = new PipedOutputStream(inputStream2);

        csvFilenameIn = dataMgrProps.getProperty("csv.infilename");

        csvSource = new CSVSource(csvFilenameIn, outputStream1);
        LOG.info("CSVSource starting CSVSource: " + csvFilenameIn);

        new Thread(
                new Runnable() {
                    public void run() {
                        csvSource.putDataOnOutputStream();
                    }
                }
        ).start();

        recordCountCSVIn = Files.lines(Paths.get(csvFilenameIn)).count();

        //kinesis producer, read from input stream / write to kinesis stream (producer)
        LOG.info("KinesisSourceTest starting Kinesis Target (producer). ");
        kwProp = new Properties();
        kwProp.setProperty("kinesis.streamname", dataMgrProps.getProperty("kinesis.streamname"));
        kwProp.setProperty("kinesis.streamsize", dataMgrProps.getProperty("kinesis.streamsize"));
        kwProp.setProperty("kinesis.region", dataMgrProps.getProperty("kinesis.region"));
        kwProp.setProperty("kinesis.partitionkey", dataMgrProps.getProperty("kinesis.partitionkey"));
        // kwProp.setProperty("kinesis.maxrecordstoprocess", dataMgrProps.getProperty("kinesis.maxrecordstoprocess"));
        kwProp.setProperty("kinesis.maxrecordstoprocess", String.valueOf(maxRecordsToProcess));

        try {
            kWriter = new KinesisTarget(kwProp, inputStream1);
            new Thread(
                    new Runnable() {
                        public void run() {
                            kWriter.processDataFromInputStream();
                        }
                    }
            ).start();
        } catch (InterruptedException ex) {
            Logger.getLogger(KinesisTarget.class.getName()).log(Level.SEVERE, null, ex);
        }

        //kinesis consumer, read from kinesis stream / write to output stream
        LOG.info("KinesisSourceTest starting KinesisSource (consumer). ");

        krProp = new Properties();
        krProp.setProperty("kinesis.streamname", dataMgrProps.getProperty("kinesis.streamname"));
        krProp.setProperty("kinesis.streamsize", dataMgrProps.getProperty("kinesis.streamsize"));
        krProp.setProperty("kinesis.region", dataMgrProps.getProperty("kinesis.region"));
        krProp.setProperty("kinesis. partitionkey", dataMgrProps.getProperty("kinesis.partitionkey"));
        krProp.setProperty("kinesis.initialpositioninstream", dataMgrProps.getProperty("kinesis.initialpositioninstream"));
        krProp.setProperty("kinesis.applicationname", dataMgrProps.getProperty("kinesis.applicationname"));
        krProp.setProperty("kinesis.endpoint", dataMgrProps.getProperty("kinesis.endpoint"));
        //krProp.setProperty("kinesis.maxrecordstoprocess", dataMgrProps.getProperty("kinesis.maxrecordstoprocess"));
        krProp.setProperty("kinesis.maxrecordstoprocess", String.valueOf(maxRecordsToProcess));

        try {
            kReader = new KinesisSource(krProp, outputStream2);
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
        //csv, read from input stream / write to  csv
        LOG.info("KinesisSourceTest starting csv target. ");
        csvFilenameOut=dataMgrProps.getProperty("csv.outfilename");
        csvTarget = new CSVTarget(csvFilenameOut, inputStream2);
        csvTarget.processDataFromInputStream();

        recordCountCSVOut = Files.lines(Paths.get(csvFilenameOut)).count();

        assertEquals(maxRecordsToProcess, recordCountCSVOut);
        LOG.info("KinesisSourceTest completed");
    }

}