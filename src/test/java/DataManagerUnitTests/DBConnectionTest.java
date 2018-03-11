package DataManagerUnitTests;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

 
import com.plesba.datamanager.DataManager;
import com.plesba.datamanager.utils.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.DateTimeException;
import java.util.*; 
import java.time.format.DateTimeFormatter;
import java.time.LocalDate;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.logging.Level;
import java.util.logging.Logger;
import static org.junit.Assert.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/**
 *
 * @author REnee
 */
public class DBConnectionTest {
    private final String propertiesFile;
    private DBConnection dbConnection;
    private static Properties dataMgrProps;
    private Connection connection;
    private static final Log LOG = LogFactory.getLog(DataManager.class);

    
    public DBConnectionTest() {
        propertiesFile =  "/Users/renee/IdeaProjects/DataManager/config.properties";
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
     * Test of getConnection method, of class DBConnection.
     */
    @Test
    public void testGetConnection() {
        LOG.info("DBConnection.testGetConnection starting");
 
       DateTimeFormatter dtf = null;
       Date dateResult = null;
       String expDateString = null;
       String dateResultString = null;
       LocalDate systemDate  = null;
       
        dataMgrProps = new DMProperties(propertiesFile).getProp();
        dbConnection = new DBConnection(dataMgrProps);
        connection = dbConnection.getConnection();

        try {
            dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");           
            systemDate = LocalDate.now();
            expDateString = dtf.format(systemDate);
            LOG.info("Java result: " + expDateString);
        }
        catch (DateTimeException exc) {
            LOG.info("DBConnectionTest %s can't be formatted!%n"+expDateString);
            throw exc;
        }

        try { 
            
            String query = "SELECT current_date";
            // execute query
            Statement statement = connection.createStatement ();
            ResultSet rs = statement.executeQuery (query); 
            
            // return query result
            while ( rs.next () ) {
                
                dateResult = rs.getDate("date");    
                dateResultString = dateResult.toString();
                LOG.info("DBConnectionTest Query result: " + dateResultString);
  
            } 
          
        }
        catch (java.sql.SQLException e ) {
            System.err.println (e); 
            e.printStackTrace();    
        }
        LOG.info("DBConnectionTest successful");
        assertEquals(expDateString, dateResultString);  
    }

}
