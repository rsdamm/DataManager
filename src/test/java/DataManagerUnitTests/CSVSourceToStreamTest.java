/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package DataManagerUnitTests;

 
import com.plesba.datamanager.DataManager;
import com.plesba.datamanager.source.CSVSourceToStream;
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
public class CSVSourceToStreamTest {
    private static PipedOutputStream outputStream1 = null;
    private static PipedInputStream inputStream1 = null;
    private static String propertiesFile = "/Users/renee/IdeaProjects/DataManager/config.properties";
    private static CSVSourceToStream csvSource = null;
    private long recordCount = 0;
    private static final Log LOG = LogFactory.getLog(DataManager.class);
    private static String csvFilename;
    private static Properties dataMgrProps = null;

    private static Properties dbProp;
    public CSVSourceToStreamTest() {
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
        

        LOG.info("CSVSourceToStreamTest starting");

        dataMgrProps = new DMProperties(propertiesFile).getProp();
        LOG.info("CVSourceTest properties obtained");

        inputStream1 = new PipedInputStream();
        outputStream1 = new PipedOutputStream(inputStream1);
        csvFilename = dataMgrProps.getProperty("csv.infilename");
        LOG.info("CSVSourceToStreamTest input from DB file: " + csvFilename);

        csvSource = new CSVSourceToStream(csvFilename, outputStream1);
        csvSource.putDataOnOutputStream();

        recordCount = Files.lines(Paths.get(csvFilename)).count();
        LOG.info("CSVSourceToStreamTest Input file record count : " + recordCount);
        
        int result = csvSource.getReadCount();
        LOG.info("CSVSourceToStreamTest Loaded " + result + " records");

        LOG.info("CSVSourceToStreamTest Stream written count: " + result);
        assertEquals(recordCount, result);
        LOG.info("CSVSourceToStreamTest completed");
    }
    
}