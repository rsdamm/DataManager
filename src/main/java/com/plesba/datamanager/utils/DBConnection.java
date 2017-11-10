
package com.plesba.datamanager.utils;

import java.sql.*;

public class DBConnection {
    DBProperties dbProps = null;
    String propertyFileName;
    Connection connection = null;
    String driverName = null;
    String connectString = null;
    String username = null;
    String password = null;
    
    public DBConnection(String pfn) 
    {
        propertyFileName = pfn; 
        dbProps = new DBProperties(propertyFileName);
        
        // load the jdbc driver
        try {
            driverName = dbProps.getDBDriver();
            Class.forName(driverName);
        }
        catch(ClassNotFoundException ex) {
            System.out.println("Error: unable to load driver class");
            System.exit(1);
        }
        
        try {
         connectString = dbProps.getDBConnectString();
         username = dbProps.getDBUser();
         password = dbProps.getDBPassword();
         
         // open connection to database 
         connection = DriverManager.getConnection(connectString, username, password);
         connection.setAutoCommit(false);
        }
        catch (java.sql.SQLException e) {
            System.err.println (e);
            System.exit (-1);
        }
    }
    public Connection getConnection() {
        return this.connection;
    }
    public Connection getCurrentDate() {
        return this.connection;
    }
}
