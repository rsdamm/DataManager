package DataManagerUnitTests;

import com.plesba.datamanager.DataManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import com.plesba.datamanager.utils.*;
import java.util.Properties;

/**
 *
 * @author REnee
 */
public class DMPropertiesTest {
    
    private static final String propertiesFile =  "/Users/renee/IdeaProjects/DataManager/testconfig.altproperties";
    private static Properties dataMgrProps = null;
    private static final Log LOG = LogFactory.getLog(DataManager.class);
    
    public DMPropertiesTest() {
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
     * Test of getDBHost method, of class DBProperties.
     */
    @Test
    public void testGetDBHost() {
        LOG.info("DMPropertiesTest testing getDBHost");
        dataMgrProps = new DMProperties(propertiesFile).getProp();
        String expResult = "bigkittycats";
        String result = dataMgrProps.getProperty("database.host");
        assertEquals(expResult, result); 
    }

    /**
     * Test of getDBDatabase method, of class DBProperties.
     */
    @Test
    public void testGetDBDatabase() {
        LOG.info("DMPropertiesTest testing getDBDatabase");
        dataMgrProps = new DMProperties(propertiesFile).getProp();
        String expResult = "pintopony";
        String result = dataMgrProps.getProperty("database.database");
        assertEquals(expResult, result); 
    }

    /**
     * Test of getDBUser method, of class DBProperties.
     */
    @Test
    public void testGetDBUser() {
        LOG.info("DMPropertiesTest testing getDBUser");
        dataMgrProps = new DMProperties(propertiesFile).getProp();
        String expResult = "bigcat";
        String result = dataMgrProps.getProperty("database.user");
        assertEquals(expResult, result); 
    }

    /**
     * Test of getDBPassword method, of class DBProperties.
     */
    @Test
    public void testGetDBPassword() {
        LOG.info("DMPropertiesTest testing getDBPassword");
        dataMgrProps = new DMProperties(propertiesFile).getProp();
        String expResult = "youonlywish";
        String result = dataMgrProps.getProperty("database.password");
        assertEquals(expResult, result); 
    }
    /**
     * Test of getDBPort method, of class DBProperties.
     */
    @Test
    public void testGetDBPort() {
        LOG.info("DMPropertiesTest testing getDBPort");
        dataMgrProps = new DMProperties(propertiesFile).getProp();
        String expResult = "5432";
        String result = dataMgrProps.getProperty("database.port");
        assertEquals(expResult, result); 
    }

    /**
     * Test of getDBConnectString method, of class DBProperties.
     */
    @Test
    public void testGetDBConnectString() {
        LOG.info("DMPropertiesTest testing getDBConnectString");
        dataMgrProps = new DMProperties(propertiesFile).getProp();
        String expResult = "bigkittycats:5432/pintopony";
        String result = dataMgrProps.getProperty("database.host")
                +":"
                + dataMgrProps.getProperty("database.port")
                +"/"
                + dataMgrProps.getProperty("database.database");
        
        assertEquals(expResult, result); 
    }

    /**
     * Test of getDBDriver method, of class DBProperties.
     */
    @Test
    public void testGetDBDriver() {
        LOG.info("DMPropertiesTest testing getDBDriver");
        dataMgrProps = new DMProperties(propertiesFile).getProp();
        String expResult = "org.postgresql.Driver";
        String result = dataMgrProps.getProperty("database.driver");
        assertEquals(expResult, result); 
    }
        /**
     * Test of getDBDriver method, of class DBProperties.
     */
    @Test
    public void testGetFilename() {
        LOG.info("DMPropertiesTest testing testGetFilename");
        dataMgrProps = new DMProperties(propertiesFile).getProp();
        String expResult = "testpropertiesfilename.dat";
        String result = dataMgrProps.getProperty("filename");
        assertEquals(expResult, result); 
    }
    
}