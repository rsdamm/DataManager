
package DataManagerUnitTests;

        import com.plesba.datamanager.DataManager;
        import com.plesba.datamanager.source.CSVSource;
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
public class KinesisTargetTest {
    private static PipedOutputStream outputStream1 = null;
    private static PipedInputStream inputStream1 = null; 
    private static String propertiesFile = "/Users/renee/IdeaProjects/DataManager/config.properties";
    private static CSVSource csvSource = null; 
    private long recordCountCSVIn = 0;
    private long recordCountStreamOut = 0;
    private static KinesisTarget kWriter = null; 
    private static final Log LOG = LogFactory.getLog(DataManager.class);
    private static String csvFilenameIn; 
    private static Properties dataMgrProps = null;
    private static Properties kwProp; 
    public KinesisTargetTest() {
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


        LOG.info("KinesisTargetTest starting");

        dataMgrProps = new DMProperties(propertiesFile).getProp();
        LOG.info("KinesisTargetTest properties obtained");

        inputStream1 = new PipedInputStream();
        outputStream1 = new PipedOutputStream(inputStream1); 

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
        LOG.info("KinesisTargetTest starting Kinesis Target (producer). ");
        kwProp = new Properties();
        kwProp.setProperty("kinesis.streamname", dataMgrProps.getProperty("kinesis.streamname"));
        kwProp.setProperty("kinesis.streamsize", dataMgrProps.getProperty("kinesis.streamsize"));
        kwProp.setProperty("kinesis.region", dataMgrProps.getProperty("kinesis.region"));
        kwProp.setProperty("kinesis.partitionkey", dataMgrProps.getProperty("kinesis.partitionkey"));
        kwProp.setProperty("kinesis.maxrecordstoprocess", dataMgrProps.getProperty("kinesis.maxrecordstoprocess"));

        try {
            kWriter = new KinesisTarget(kwProp, inputStream1);
            kWriter.processDataFromInputStream();
            recordCountStreamOut=kWriter.GetLoadedCount();

        } catch (InterruptedException ex) {
            Logger.getLogger(KinesisTarget.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        assertEquals(recordCountCSVIn, recordCountStreamOut);
        LOG.info("KinesisTargetTest completed");
    }

}
