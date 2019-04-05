package DataManagerUnitTests;

        import com.plesba.datamanager.DataManager;
        import com.plesba.datapiper.source.CSVSourceToStream;
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
public class KafkaTargetFromStreamTest {
    private static PipedOutputStream outputStream1 = null;
    private static PipedInputStream inputStream1 = null;
    private static String propertiesFile = "/Users/renee/IdeaProjects/DataManager/config.properties";
    private static CSVSourceToStream csvSource = null;
    private long recordCountCSVIn = 0;
    private long recordCountStreamOut = 0;
    private long maxStreamCount = 0;
    private static KafkaTargetFromStream kfWriter = null;
    private static final Log LOG = LogFactory.getLog(DataManager.class);
    private static String csvFilenameIn;
    private static Properties dataMgrProps = null;
    private static Properties kfwProp;
    public KafkaTargetFromStreamTest() {
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


        LOG.info("KafkaTargetFromStreamTest starting");

        dataMgrProps = new DMProperties(propertiesFile).getProp();
        LOG.info("KafkaTargetFromStreamTest properties obtained");

        inputStream1 = new PipedInputStream();
        outputStream1 = new PipedOutputStream(inputStream1);

        csvFilenameIn = dataMgrProps.getProperty("csv.infilename");

        csvSource = new CSVSourceToStream(csvFilenameIn, outputStream1);
        LOG.info("CSVSourceToStream starting CSVSource: " + csvFilenameIn);

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
        try {
            kfWriter = new KafkaTargetFromStream(kfwProp, inputStream1);
            kfWriter.processDataFromInputStream();
        } catch (InterruptedException ex) {
            Logger.getLogger(KafkaTargetFromStreamTest.class.getName()).log(Level.SEVERE, null, ex);
        }

        maxStreamCount =  Integer.parseInt(dataMgrProps.getProperty("kafka.maxrecordstoprocess"));
        if (maxStreamCount >0)
        {
            assertEquals(maxStreamCount, recordCountStreamOut);
        }
        else {

            assertEquals(recordCountCSVIn, recordCountStreamOut);
        }


        LOG.info("KafkaTargetFromStreamTest completed");
    }

}

