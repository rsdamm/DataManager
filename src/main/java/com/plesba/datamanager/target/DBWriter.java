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
import java.util.Arrays;
import java.util.List;

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
    private List<String> cols ;

    public DBWriter(Connection parameterConnection, PipedInputStream parameterInputStream) {
        inputStream = parameterInputStream;
        connection = parameterConnection;

    }

    public DBWriter() {
        inputStream = null;
        connection = null;
    }

    public void processDataFromInputStream() throws IOException {

        try { 

            
            StringBuilder recordStringBuffer = new StringBuilder(); 
            String streamRecord = new String();
            stmt = connection.prepareStatement(insert_dml);
            //Read one line at a time

            int streamByte = inputStream.read();

            while (streamByte != -1) {  
                                        
                    if (streamByte != 10) {
                        recordStringBuffer.append((char) streamByte);
                    } else {
                        cols = Arrays.asList(recordStringBuffer.toString().split(","));
                        stmt.setString(1, cols.get(0));
                        stmt.setString(2, cols.get(1));
                        stmt.executeUpdate(); 
                        recordCount++;
                        streamRecord =recordStringBuffer.toString();
                        System.out.println("Processed record: " + streamRecord);                       
                        recordStringBuffer.setLength(0);
                    }    
                 streamByte = inputStream.read();

                }
            
                connection.commit();
                System.out.println("Inserted records: " + recordCount);
             
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
