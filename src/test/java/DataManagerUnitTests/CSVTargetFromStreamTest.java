package DataManagerUnitTests;


import com.plesba.datamanager.DataManager;
import com.plesba.datapiper.source.CSVSourceToStream;
import com.plesba.datapiper.target.CSVTargetFromStream;
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
public class CSVTargetFromStreamTest {
    private static PipedOutputStream outputStream1 = null;
    private static PipedInputStream inputStream1 = null;
    private static String propertiesFile = "/Users/renee/IdeaProjects/DataManager/config.properties";
    private static CSVSourceToStream csvSourceToStream = null;
    private static CSVTargetFromStream csvTargetFromStream = null;
    private long recordCountIn = 0;
    private long recordCountOut = 0;
    private static final Log LOG = LogFactory.getLog(DataManager.class);
    private static String csvOutfilename;
    private static String csvInfilename;
    private static Properties dataMgrProps = null;

    private static Properties dbProp;
    public CSVTargetFromStreamTest() {
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


        LOG.info("CSVTargetFromStreamTest starting");

        dataMgrProps = new DMProperties(propertiesFile).getProp();
        LOG.info("CSVTargetFromStreamTest starting properties obtained");

        inputStream1 = new PipedInputStream();
        outputStream1 = new PipedOutputStream(inputStream1);

        csvInfilename = dataMgrProps.getProperty("csv.infilename");
        LOG.info("CSVTargetFromStreamTest input file: " + csvInfilename);

        csvOutfilename = dataMgrProps.getProperty("csv.outfilename");
        LOG.info("CSVTargetFromStreamTest output file: " + csvOutfilename);

        csvSourceToStream = new CSVSourceToStream(csvInfilename, outputStream1);
        csvSourceToStream.putDataOnOutputStream();

        recordCountIn = Files.lines(Paths.get(csvInfilename)).count();
        LOG.info("CSVTargetFromStreamTest Input file record count : " + recordCountIn);

        csvTargetFromStream = new CSVTargetFromStream(csvOutfilename, inputStream1);
        csvTargetFromStream.processDataFromInputStream();

        recordCountOut = Files.lines(Paths.get(csvOutfilename)).count();
        LOG.info("CSVTargetFromStreamTest output file record count : " + recordCountOut);

        assertEquals(recordCountIn, recordCountOut);
        LOG.info("CSVTargetFromStreamTest completed");
    }

}