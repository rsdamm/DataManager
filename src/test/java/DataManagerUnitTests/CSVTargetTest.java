package DataManagerUnitTests;


import com.plesba.datamanager.DataManager;
import com.plesba.datamanager.source.CSVSource;
import com.plesba.datamanager.target.CSVTarget;
import com.plesba.datamanager.utils.DMProperties;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
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
public class CSVTargetTest {
    private static PipedOutputStream outputStream1 = null;
    private static PipedInputStream inputStream1 = null;
    private static String propertiesFile = "/Users/renee/IdeaProjects/DataManager/config.properties";
    private static CSVSource csvSource = null;
    private static CSVTarget csvTarget = null;
    private long recordCountIn = 0;
    private long recordCountOut = 0;
    private static final Log LOG = LogFactory.getLog(DataManager.class);
    private static String csvOutfilename;
    private static String csvInfilename;
    private static Properties dataMgrProps = null;

    private static Properties dbProp;
    public CSVTargetTest() {
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


        LOG.info("CSVTargetTest starting");

        dataMgrProps = new DMProperties(propertiesFile).getProp();
        LOG.info("CSVTargetTest starting properties obtained");

        inputStream1 = new PipedInputStream();
        outputStream1 = new PipedOutputStream(inputStream1);

        csvInfilename = dataMgrProps.getProperty("csv.infilename");
        LOG.info("CSVTargetTest input file: " + csvInfilename);

        csvOutfilename = dataMgrProps.getProperty("csv.outfilename");
        LOG.info("CSVTargetTest output file: " + csvOutfilename);

        csvSource = new CSVSource(csvInfilename, outputStream1);
        csvSource.putDataOnOutputStream();

        recordCountIn = Files.lines(Paths.get(csvInfilename)).count();
        LOG.info("CSVTargetTest Input file record count : " + recordCountIn);

        csvTarget = new CSVTarget(csvOutfilename, inputStream1);
        csvTarget.processDataFromInputStream();

        recordCountOut = Files.lines(Paths.get(csvOutfilename)).count();
        LOG.info("CSVTargetTest output file record count : " + recordCountOut);

        assertEquals(recordCountIn, recordCountOut);
        LOG.info("CSVTargetTest completed");
    }

}