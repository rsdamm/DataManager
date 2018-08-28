package DataManagerUnitTests;

import com.plesba.datamanager.DataManager;
import com.plesba.datamanager.source.CSVSource;
import com.plesba.datamanager.target.CSVTarget;
import com.plesba.datamanager.utils.DMProperties;
import com.plesba.datamanager.transformers.NullTransformer;
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

public class NullTransformerTest {
    private static PipedOutputStream outputStream1 = null;
    private static PipedInputStream inputStream1 = null;
    private static PipedOutputStream outputStream2 = null;
    private static PipedInputStream inputStream2 = null;
    private static String propertiesFile = "/Users/renee/IdeaProjects/DataManager/config.properties";
    private static CSVSource csvSource = null;
    private static CSVTarget csvTarget = null;
    private static NullTransformer nullTransformer = null;
    private long recordCountIn = 0;
    private long recordCountOut = 0;
    private static final Log LOG = LogFactory.getLog(DataManager.class);
    private static String csvOutfilename;
    private static String csvInfilename;
    private static Properties dataMgrProps = null;
    private long transformCount = 0;

    private static Properties dbProp;
    public NullTransformerTest() {
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


        LOG.info("NullTransformerTest starting");

        dataMgrProps = new DMProperties(propertiesFile).getProp();
        LOG.info("NullTransformerTest starting properties obtained");

        inputStream1 = new PipedInputStream();
        outputStream1 = new PipedOutputStream(inputStream1);
        inputStream2 = new PipedInputStream();
        outputStream2 = new PipedOutputStream(inputStream2);

        csvInfilename = dataMgrProps.getProperty("csv.infilename");
        LOG.info("NullTransformerTest input file: " + csvInfilename);

        csvOutfilename = dataMgrProps.getProperty("csv.outfilename");
        LOG.info("NullTransformerTest output file: " + csvOutfilename);

        csvSource = new CSVSource(csvInfilename, outputStream1);
        csvSource.putDataOnOutputStream();

        recordCountIn = Files.lines(Paths.get(csvInfilename)).count();
        LOG.info("NullTransformerTest Input file record count : " + recordCountIn);

        nullTransformer = new NullTransformer(inputStream1, outputStream2);
        nullTransformer.processDataFromInputStream();

        transformCount = nullTransformer.getTransformedCount();
        LOG.info("NullTransformerTest transformer processed count : " + transformCount);

        csvTarget = new CSVTarget(csvOutfilename, inputStream2);
        csvTarget.processDataFromInputStream();

        recordCountOut = Files.lines(Paths.get(csvOutfilename)).count();
        LOG.info("Output file record count : " + recordCountOut);

        assertEquals(recordCountIn, recordCountOut);
        LOG.info("NullTransformerTest completed");
    }

}
