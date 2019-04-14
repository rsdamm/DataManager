package DataManagerUnitTests;

import com.plesba.datamanager.DataManager;
import com.plesba.datapiper.source.DBSourceToStream;
import com.plesba.datamanager.utils.DMProperties;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.sql.Connection;
import java.util.Properties;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import com.plesba.datamanager.utils.DBConnection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
public class DBSourceToStreamTest {

    private static PipedOutputStream outputStream1 = null;
    private static PipedInputStream inputStream1 = null;
    private static String propertiesFile = "/Users/renee/IdeaProjects/DataManager/config.properties";
    private static Properties dbProp = null;
    private long recordCount = 0;
    private static final Log LOG = LogFactory.getLog(DataManager.class);
    private static DBSourceToStream dbReader = null;
    private static DBConnection dbConnection = null;
    private static Connection connection = null;

    public DBSourceToStreamTest() {
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


        System.out.println("DBSourceToStreamTest starting");

        dbProp = new DMProperties(propertiesFile).getProp();
        LOG.info("DBSourceToStreamTest properties obtained");

        inputStream1 = new PipedInputStream();
        outputStream1 = new PipedOutputStream(inputStream1);

        dbConnection = new DBConnection(dbProp);
        connection = dbConnection.getConnection();

        dbReader = new DBSourceToStream(connection, outputStream1);
        dbReader.processDataFromDB();

        recordCount = dbReader.getRecordCountInTable();
        LOG.info("DBSourceToStreamTest record count : " + recordCount);

        int result = dbReader.GetQueryResultCount();
        LOG.info("DBSourceToStreamTest records read from table " + result);

        LOG.info("DBSourceToStreamTest records in table " + recordCount);

        assertEquals(recordCount, result);
        LOG.info("DBSourceToStreamTest completed");
    }

}
