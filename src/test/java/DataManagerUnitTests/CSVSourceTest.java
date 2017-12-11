/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package DataManagerUnitTests;

 
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
/**
 *
 * @author REnee
 */
public class CSVSourceTest {

    private static final String propertiesFile = "/home/renee/git/DataManager/config.properties";
    private static Properties dataMgrProps = null; 
    private static PipedOutputStream outputStream = null;
    private static PipedInputStream inputStream = null;
    private static CSVSource csvReader = null; 
    long recordCount = 0;
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
        
        System.out.println("CSVSourceTest properties obtained");
        
        inputStream = new PipedInputStream();
        outputStream = new PipedOutputStream(inputStream);
        
        String loadFile = dataMgrProps.getProperty("filename");
        
        System.out.println("CSVSourceTest Reading file: " + loadFile);
        csvReader = new CSVSource(loadFile, outputStream); 
        
        new Thread(csvReader::putDataOnOutputStream).start();
        
        recordCount = Files.lines(Paths.get(loadFile)).count();
        
        
        System.out.println("CSVSourceTest Input file record count : " + recordCount);
        
        int result = csvReader.GetReadCount();
        System.out.println("Loaded " + result + " records");
        
        System.out.println("CSVSourceTest Stream written count: " + result);
        assertEquals(recordCount, result);
        System.out.println("CSVSourceTest completed");
    }
    
}