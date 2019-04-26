package com.plesba.datamanager.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.util.Properties;
import java.sql.*;

public class DBConnection {

    private Connection connection = null;
    private Properties dbProperties = null;
    private String user= null;
    private String password= null;
    private String driver= null;
    private String database= null;
    private String host= null;
    private String port= null;
    private String connectString= null; //generated
    private Log LOG = LogFactory.getLog(DBConnection.class);

    public DBConnection(Properties parameterProperties) {

        LOG.info("DBConnection setup started.");

        String userOverride = parameterProperties.getProperty("database.user");
        if(userOverride !=null) {
            user = userOverride;
        }

        LOG.info("DBConnection user  "+user);

        String passwordOverride = parameterProperties.getProperty("database.password");
        if(passwordOverride !=null) {
            password = passwordOverride;
        }

        LOG.info("DBConnection password  "+password);

        String driverOverride = parameterProperties.getProperty("database.driver");
        if(driverOverride !=null) {
            driver = driverOverride;
        }

        LOG.info("DBConnection driver  "+driver);

        String databaseOverride = parameterProperties.getProperty("database.database");
        if(databaseOverride !=null)
        {
            database = databaseOverride;
        }

        LOG.info("DBConnection database  "+database);

        String hostOverride = parameterProperties.getProperty("database.host");
        if(hostOverride !=null) {
            host = hostOverride;
        }

        LOG.info("DBConnection host  "+host);

        String portOverride = parameterProperties.getProperty("database.port");
        if(portOverride !=null) {
            port = portOverride;
        }

        LOG.info("DBConnection host  "+host);
        connectString = host + ":" + port + "/" + database;
        LOG.info("DBConnection connect string  "+connectString);

        connect();
    }
    public String getUserName() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getDriver() {
        return driver;
    }

    public String getDatabase() {
        return database;
    }

    public String getPort() {
        return port;
    }

    public String getHost() {
        return host;
    }

    public String getConnectString() {
        return connectString;
    }

    public Connection getConnection() {return this.connection;}

    private void connect() {
            // load the jdbc driver
            try {
                Class.forName(driver);
                // open connection to database 
                connection = DriverManager.getConnection(connectString, user, password);
                connection.setAutoCommit(false);
            } catch (ClassNotFoundException ex) {
                LOG.info("DBConnection error: unable to load driver class");
                System.exit(1);
            } catch (java.sql.SQLException e) {
                System.err.println(e);
                System.exit(-1);
            }
    }
    public void closeConnection() {
        // load the jdbc driver
        try {

            if (connection != null) {
                connection.close();
                LOG.info("DBConnection.closeConnection connection closed.");
            }
        }
        catch (java.sql.SQLException e) {
            System.err.println (e);
            e.printStackTrace();
        }
    }

}
