/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.plesba.datamanager.target;

import java.io.FileReader;
import java.io.IOException;
import java.io.PipedInputStream;
import java.sql.*;

/**
 *
 * @author renee
 */
public class DBWriter {

    private final Connection connection;
    private final PipedInputStream inputStream;
    private final String insert_dml = "INSERT INTO ham_call_signs (NAME, CALL_SIGN) VALUES (?, ?)";
    private PreparedStatement stmt = null;
    private int recordCount = 0;

    public DBWriter(Connection parameterConnection, PipedInputStream parameterInputStream) {
        inputStream = parameterInputStream;
        connection = parameterConnection;

    }

    public DBWriter() {
        inputStream = null;
        connection = null;
    }

    public void getDataFromInputStream() throws IOException {

        try {

            stmt = connection.prepareStatement(insert_dml);
            //Read one line at a time

            int streamRecord = inputStream.read();

            while (streamRecord != -1) {

                {
                    //stmt.setString(1, nextLine[0]);
                    //stmt.setString(2, nextLine[1]);

                        System.out.println("Inserting: " + streamRecord);
                    //stmt.executeUpdate();
                    recordCount++;
                    
                    streamRecord = inputStream.read();

                }
                connection.commit();
                System.out.println("Inserted records: " + recordCount);

                //getRecords();
            }
        }
            catch (java.sql.SQLException e) {
            System.err.println (e);
            e.printStackTrace();
        }
            //close the connection
            finally {

            try {
                if (stmt != null) {
                    stmt.close();
                }

            if (connection != null) {
		connection.close();
		}
            }
            catch (java.sql.SQLException e) {
            System.err.println (e);
            e.printStackTrace();
        }
            
	}
        }
    

    public int GetLoadedCount() {
        return this.recordCount;
    }

    public void getRecords() {
        String rowName = null;
        String rowCallSign = null;

        try {

            String query = "SELECT name, call_sign FROM ham_call_signs";
            // execute query
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(query);

            // return query result
            while (rs.next()) {

                rowName = rs.getString("name");
                rowCallSign = rs.getString("call_sign");
                System.out.println("Query result: " + rowName + " - " + rowCallSign);
            }
            System.out.println("Read records: " + recordCount);

        } catch (java.sql.SQLException e) {
            System.err.println(e);
            e.printStackTrace();
        }
    }
}
