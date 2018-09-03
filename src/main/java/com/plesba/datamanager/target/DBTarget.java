/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.plesba.datamanager.target;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.FileReader;
import java.io.IOException;
import java.io.PipedInputStream;
import java.sql.*;
import java.util.Arrays;
import java.util.List;

/**
 *
 * @author renee
 *
 *   Reads input stream writes to PostgreSQL database
 */
public class DBTarget {

    private final Connection connection;
    private PipedInputStream inputStream;
    private final String insert_dml = "INSERT INTO ham_call_signs (NAME, CALL_SIGN) VALUES (?, ?)";
    private PreparedStatement stmt = null;
    private int recordCount = 0;
    private List<String> cols ;
    private int tableRecordCount=0;

    private static final Log LOG = LogFactory.getLog(DBTarget.class);

    public DBTarget(Connection parameterConnection, PipedInputStream parameterInputStream) {
        inputStream = parameterInputStream;
        connection = parameterConnection;

        LOG.info("DBTarget started processing");
    }

    public DBTarget() {
        inputStream = null;
        connection = null;

        LOG.info("DBTarget started processing with no parameters");
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
                    streamRecord = recordStringBuffer.toString();

                    LOG.info("DBTarget processed " + streamRecord);
                    recordStringBuffer.setLength(0);
                }
                streamByte = inputStream.read();

            }

            connection.commit();

            LOG.info("DBTarget processed all records from input stream; Records inserted to database: " + recordCount);

        } catch (java.sql.SQLException e) {
            System.err.println(e);
            e.printStackTrace();
        }

        //close the connection
        finally {

            try {
                if (stmt != null) {
                    stmt.close();
                }

            } catch (java.sql.SQLException e) {
                System.err.println(e);
                e.printStackTrace();
            }
        }
    }

    public int GetRecordCountInsertedInDB() {
        return this.recordCount;
    }

    public int getRecordCountInTable() throws IOException {

        try {

            LOG.info("DBTarget.getRecordCountInTable starting " + recordCount);
            try {

                String query = "SELECT count(*) as record_count FROM ham_call_signs";
                // execute query
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(query);

                // return query result
                while (rs.next()) {

                    tableRecordCount = Integer.parseInt(rs.getString("record_count"));

                }
                LOG.info("DBTarget.getRecordCountInTable finished processing " + recordCount);


            } catch (java.sql.SQLException e) {
                System.err.println(e);
                e.printStackTrace();
            }


        } catch (Exception e) {
            System.err.println(e);
            e.printStackTrace();
        }
        //cleanup
        finally {

            try {
                if (stmt != null) {
                    stmt.close();
                }

            } catch (java.sql.SQLException e) {
                System.err.println(e);
                e.printStackTrace();
            }
            return this.tableRecordCount;
        }
    }
}
