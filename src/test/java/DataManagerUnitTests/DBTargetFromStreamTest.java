package DataManagerUnitTests;

import com.plesba.datamanager.DataManager;
import com.plesba.datamanager.source.CSVSourceToStream;
import com.plesba.datamanager.target.DBTargetFromStream;
import com.plesba.datamanager.utils.DBConnection;
import com.plesba.datamanager.utils.DMProperties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.*;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class DBTargetFromStreamTest {

    private static PipedOutputStream outputStream1 = null;
    private static PipedInputStream inputStream1 = null;
    private static String propertiesFile = "/Users/renee/IdeaProjects/DataManager/config.properties";
    private static Properties dbProp = null;
    private long csvRecordCount = 0;
    private long recordCountAfter = 0;
    private static final Log LOG = LogFactory.getLog(DataManager.class);
    private static DBTargetFromStream dbLoader = null;
    private static DBConnection dbConnection = null;
    private static Connection connection;

    private static CSVSourceToStream csvSource = null;

    public DBTargetFromStreamTest() {
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
     *
     * @throws java.io.IOException
     */
    @Test
    public void testRun() throws IOException {

        LOG.info("DBTargetFromStreamTest starting");

        dbProp = new DMProperties(propertiesFile).getProp();
        LOG.info("DBTargetFromStreamTest properties obtained");

        inputStream1 = new PipedInputStream();
        outputStream1 = new PipedOutputStream(inputStream1);

        csvSource = new CSVSourceToStream(dbProp.getProperty("csv.infilename"), outputStream1);
        csvSource.putDataOnOutputStream();

        csvRecordCount = Files.lines(Paths.get(dbProp.getProperty("csv.infilename"))).count();
        LOG.info("CSVSourceToStreamTest Input file record count : " + csvRecordCount);

        dbConnection = new DBConnection(dbProp);
        Connection connection = connection = dbConnection.getConnection();


        dbLoader = new DBTargetFromStream(connection, inputStream1);

        int dbrecordCountBefore = dbLoader.getRecordCountInTable();
        LOG.info("DBTargetFromStreamTest record count : " + csvRecordCount);

        dbLoader.processDataFromInputStream();

        int dbRecordCountAfter = dbLoader.getRecordCountInTable();
        LOG.info("DBTargetFromStreamTest records written to table " + (dbRecordCountAfter - dbrecordCountBefore));
 
        LOG.info("CSVSourceToStreamTest Input file record count : " + recordCountAfter);

        dbConnection.closeConnection();

        assertEquals(dbRecordCountAfter - dbrecordCountBefore, csvRecordCount);
        LOG.info("DBTargetFromStreamTest completed");
    }

}
