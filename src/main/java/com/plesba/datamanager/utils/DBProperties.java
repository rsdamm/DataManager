package com.plesba.datamanager.utils;
 
import java.util.Properties; 
import java.io.FileInputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.InputStream;

public class DBProperties {

        //Configuration config = null;
        Properties prop = new Properties();
        InputStream input = null;
        
        String dbHost = null;
        String dbUser = null;
        String dbPassword = null;
        String dbDatabase = null;
        String dbConnectString = null;
        String dbPort = null; 
        String jdbcDriver = null; 
        String propFile = null;
         
       public DBProperties (String pfn) {
        
           propFile = pfn;        

        try {
                  
            prop.load(new FileInputStream(propFile));
            
            jdbcDriver = prop.getProperty("database.driver");
            dbHost = prop.getProperty("database.host");
            dbUser = prop.getProperty("database.user");
            dbPassword = prop.getProperty("database.password");
            dbDatabase = prop.getProperty("database.database");
            dbPort = prop.getProperty("database.port");
            dbConnectString = dbHost + ":" + dbPort + "/" + dbDatabase;
            
    //        System.out.println("Host: " + dbHost);
    //        System.out.println("database: " + dbDatabase);
    //        System.out.println("user: " + dbUser);
    //        System.out.println("password: " + dbPassword);
    //        System.out.println("connect string: " + dbConnectString);
    //        System.out.println("driver: " + jdbcDriver);
    //        System.out.println("port: " + dbPort);
            
        } catch(FileNotFoundException ex ) {
                System.out.println("Error: unable to open properties");
                ex.printStackTrace();
        } catch(IOException ex) {
                System.out.println("Error: unable to read properties file");
             ex.printStackTrace();
        } finally {
		if (input != null) {
			try {
				input.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
         
                }
        }
    }

    public String getDBHost() {
        return this.dbHost;
    }      
    public String getDBDatabase() {
        return this.dbDatabase;
    }       
    public String getDBUser() {
        return this.dbUser;
    }   
    public String getDBPassword() {
        return this.dbPassword;
    }     
    public String getDBPort() {
        return this.dbPort;
    }  
    public String getDBConnectString() {
        return this.dbConnectString;
    } 
     public String getDBDriver() {
        return this.jdbcDriver;
    }
}
