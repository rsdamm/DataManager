package DataManagerUnitTests;

import com.plesba.datamanager.DataManager;
import com.plesba.datapiper.source.CSVSourceToStream;
import com.plesba.datapiper.target.CSVTargetFromStream;
import com.plesba.datapiper.transformers.ReverseTransformer;
import com.plesba.datamanager.utils.DMProperties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.*;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class ReverseTransformerTest {
    private static PipedOutputStream outputStream1 = null;
    private static PipedInputStream inputStream1 = null;
    private static PipedOutputStream outputStream2 = null;
    private static PipedInputStream inputStream2 = null;
    private static String propertiesFile = "/Users/renee/IdeaProjects/DataManager/config.properties";
    private static CSVSourceToStream csvSource = null;
    private static CSVTargetFromStream csvTargetFromStream = null;
    private static ReverseTransformer nullTransformer = null;
    private long recordCountIn = 0;
    private long recordCountOut = 0;
    private static final Log LOG = LogFactory.getLog(DataManager.class);
    private static String csvOutfilename;
    private static String csvInfilename;
    private static Properties dataMgrProps = null;
    private long transformCount = 0;

    private static Properties dbProp;
    public ReverseTransformerTest() {
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
     * @throws IOException
     */
    @Test
    public void testRun() throws IOException {


        LOG.info("ReverseTransformerTest starting");

        dataMgrProps = new DMProperties(propertiesFile).getProp();
        LOG.info("ReverseTransformerTest starting properties obtained");

        inputStream1 = new PipedInputStream();
        outputStream1 = new PipedOutputStream(inputStream1);
        inputStream2 = new PipedInputStream();
        outputStream2 = new PipedOutputStream(inputStream2);

        csvInfilename = dataMgrProps.getProperty("csv.infilename");
        LOG.info("ReverseTransformerTest input file: " + csvInfilename);

        csvOutfilename = dataMgrProps.getProperty("csv.outfilename");
        LOG.info("ReverseTransformerTest output file: " + csvOutfilename);

        csvSource = new CSVSourceToStream(csvInfilename, outputStream1);
        csvSource.putDataOnOutputStream();

        recordCountIn = Files.lines(Paths.get(csvInfilename)).count();
        LOG.info("ReverseTransformerTest Input file record count : " + recordCountIn);

        nullTransformer = new ReverseTransformer(inputStream1, outputStream2);
        nullTransformer.processDataFromInputStream();

        transformCount = nullTransformer.getTransformedCount();
        LOG.info("ReverseTransformerTest transformer processed count : " + transformCount);

        csvTargetFromStream = new CSVTargetFromStream(csvOutfilename, inputStream2);
        csvTargetFromStream.processDataFromInputStream();

        recordCountOut = Files.lines(Paths.get(csvOutfilename)).count();
        LOG.info("Output file record count : " + recordCountOut);

        assertEquals(recordCountIn, recordCountOut);
        LOG.info("ReverseTransformerTest completed");
    }

}
