package DataManagerUnitTests;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

 
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
import static org.junit.Assert.*;
import com.plesba.datamanager.utils.*;

/**
 *
 * @author REnee
 */
public class DBConnectionTest {
    private final String propertiesFile;
    private DBConnection dbSetup; 
    private static Properties dataMgrProps;
    private Connection connection;
    
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
       System.out.println("testGetConnection");
 
       DateTimeFormatter dtf = null;
       Date dateResult = null;
       String expDateString = null;
       String dateResultString = null;
       LocalDate systemDate  = null;
       
        dataMgrProps = new DMProperties(propertiesFile).getProp(); 
        dbSetup = getDB(); 
        try {
            dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");           
            systemDate = LocalDate.now();
            expDateString = dtf.format(systemDate);
            System.out.println ("Java result: " + expDateString); 
        }
        catch (DateTimeException exc) {
            System.out.printf("%s can't be formatted!%n", expDateString);
            throw exc;
        }

        try { 
            
            String query = "SELECT current_date";
            connection = dbSetup.getConnection();
            // execute query
            Statement statement = connection.createStatement ();
            ResultSet rs = statement.executeQuery (query); 
            
            // return query result
            while ( rs.next () ) {
                
                dateResult = rs.getDate("date");    
                dateResultString = dateResult.toString();
                System.out.println ("Query result: " + dateResultString);   
  
            } 
          
        }
        catch (java.sql.SQLException e ) {
            System.err.println (e); 
            e.printStackTrace();    
        }   
         System.out.println ("DBConnectionTest successful");
        assertEquals(expDateString, dateResultString);  
    }
    public static DBConnection getDB(){
    
        return new DBConnection.ConnectionBuilder()
                .user(dataMgrProps.getProperty("database.user"))
                .password(dataMgrProps.getProperty("database.password"))
                .database(dataMgrProps.getProperty("database.database"))
                .port(dataMgrProps.getProperty("database.port"))
                .driver(dataMgrProps.getProperty("database.driver"))
                .host(dataMgrProps.getProperty("database.host"))
                .build();
    } 
}
