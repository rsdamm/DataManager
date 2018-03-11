package DataManagerUnitTests;

import com.plesba.datamanager.DataManager;
import com.plesba.datamanager.source.CSVSource;
import com.plesba.datamanager.source.DBSource;
import com.plesba.datamanager.utils.DMProperties;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
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
public class DBSourceTest {

    private static PipedOutputStream outputStream1 = null;
    private static PipedInputStream inputStream1 = null;
    private static String propertiesFile = "/Users/renee/IdeaProjects/DataManager/config.properties";
    private static Properties dbProp = null;
    private long recordCount = 0;
    private static final Log LOG = LogFactory.getLog(DataManager.class);
    private static DBSource dbReader = null;
    private static DBConnection dbConnection = null;
    private static Connection connection = null;

    public DBSourceTest() {
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


        System.out.println("DBSourceTest starting");

        dbProp = new DMProperties(propertiesFile).getProp();
        LOG.info("DBSourceTest properties obtained");

        inputStream1 = new PipedInputStream();
        outputStream1 = new PipedOutputStream(inputStream1);

        dbConnection = new DBConnection(dbProp);
        connection = dbConnection.getConnection();

        dbReader = new DBSource(connection, outputStream1);
        dbReader.processDataFromDB();

        recordCount = dbReader.getRecordCountInTable();
        LOG.info("DBSourceTest record count : " + recordCount);

        int result = dbReader.GetQueryResultCount();
        LOG.info("DBSourceTest records read from table " + result);

        LOG.info("DBSourceTest records in table " + recordCount);

        assertEquals(recordCount, result);
        LOG.info("DBSourceTest completed");
    }

}
