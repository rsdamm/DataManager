/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package DataManagerUnitTests;

 
import com.plesba.datamanager.DataManager;
import com.plesba.datamanager.source.CSVSource;
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
public class CSVSourceTest {
    private static PipedOutputStream outputStream1 = null;
    private static PipedInputStream inputStream1 = null;
    private static String propertiesFile = "/Users/renee/IdeaProjects/DataManager/config.properties";
    private static Properties dataMgrProps = null; 
    private static PipedOutputStream outputStream = null;
    private static CSVSource csvSource = null;
    private long recordCount = 0;
    private static final Log LOG = LogFactory.getLog(DataManager.class);
    private static String datasource;

    public CSVSourceTest() {
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
        
        
        System.out.println("CSVSourceTest starting");

        dataMgrProps = new DMProperties(propertiesFile).getProp();
        datasource = dataMgrProps.getProperty("dm.datasource");
        
        System.out.println("CSVSourceTest properties obtained");

        inputStream1 = new PipedInputStream();
        outputStream1 = new PipedOutputStream(inputStream1);
        datasource = dataMgrProps.getProperty("csv.infilename");
        LOG.info("CSVSourceTest input from csv file: " + datasource);

        csvSource = new CSVSource(datasource, outputStream1);
        csvSource.putDataOnOutputStream();

        recordCount = Files.lines(Paths.get(datasource)).count();
        System.out.println("CSVSourceTest Input file record count : " + recordCount);
        
        int result = csvSource.getReadCount();
        System.out.println("Loaded " + result + " records");
        
        System.out.println("CSVSourceTest Stream written count: " + result);
        assertEquals(recordCount, result);
        System.out.println("CSVSourceTest completed");
    }
    
}