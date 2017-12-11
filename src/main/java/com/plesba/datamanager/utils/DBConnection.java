package com.plesba.datamanager.utils;

import java.sql.*;

public class DBConnection {

    Connection connection = null;
    private final String user; //required
    private final String password; //required
    private final String driver;   //required
    private final String database; //required
    private final String connectString; //required
    private final String host; //required
    private final String port; //required

    private DBConnection(ConnectionBuilder builder) {
        this.user = builder.user;
        this.password = builder.password;
        this.driver = builder.driver;
        this.database = builder.database;
        this.port = builder.port;
        this.host = builder.host;
        this.connectString = builder.host + ":" + builder.port + "/" + builder.database;
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

    public static class ConnectionBuilder {

        private String user;
        private String password;
        private String driver;
        private String database;
        private String connectString;
        private String host;
        private String port;

        public ConnectionBuilder() {
        }

        public ConnectionBuilder user(String user) {
            this.user = user;
            return this;

        }

        public ConnectionBuilder password(String password) {
            this.password = password;
            return this;

        }

        public ConnectionBuilder port(String port) {
            this.port = port;
            return this;

        }

        public ConnectionBuilder database(String database) {
            this.database = database;
            return this;

        }

        public ConnectionBuilder driver(String driver) {
            this.driver = driver;
            return this;

        }

        public ConnectionBuilder host(String host) {
            this.host = host;
            return this;

        }

        public DBConnection build() {
            return new DBConnection(this);
        }
   }
        private void connect() {
            // load the jdbc driver
            try {
                Class.forName(driver);
                // open connection to database 
                connection = DriverManager.getConnection(connectString, user, password);
                connection.setAutoCommit(false);
            } catch (ClassNotFoundException ex) {
                System.out.println("Error: unable to load driver class");
                System.exit(1);
            } catch (java.sql.SQLException e) {
                System.err.println(e);
                System.exit(-1);
            }
        }
 
    public Connection getConnection() {
        return this.connection;
    }
}
