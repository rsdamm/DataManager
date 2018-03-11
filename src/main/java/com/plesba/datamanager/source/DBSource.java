package com.plesba.datamanager.source; 

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.PipedOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

public class DBSource {

    private final Connection connection;
    private final PipedOutputStream outputStream;
    private PreparedStatement stmt = null;
    private int recordCount = 0;
    private StringBuilder recordStringBuffer=null;
    private String dbRecord = null;
    private byte[] theByteArray = null;
    private String [] nextLine = null;
    private int i=0;
    private String rowName = null;
    private int tableRecordCount = 0;
    private String rowCallSign = null;

    private static final Log LOG = LogFactory.getLog(DBSource.class);

    public DBSource(Connection parameterConnection, PipedOutputStream parameterOutputStream) {
        outputStream = parameterOutputStream;
        connection = parameterConnection;

        LOG.info("DBSource started processing");
    }

    public DBSource() {
        outputStream = null;
        connection = null;

        LOG.info("DBSource started processing with no parameters");
    }


    public void processDataFromDB() throws IOException {
        recordStringBuffer = new StringBuilder();

        try {

            try {

                String query = "SELECT name, call_sign FROM ham_call_signs";
                // execute query
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(query);

                // return query result
                while (rs.next()) {

                    rowName = rs.getString("name");
                    rowCallSign = rs.getString("call_sign");
                    LOG.info("DBSource query result" + rowName + " - " + rowCallSign);

                    dbRecord = rowName + ',' + rowCallSign ;
                    nextLine = dbRecord.split(" ") ;

                    for (i=0; i < nextLine.length; i++) recordStringBuffer.append(nextLine[i]);

                    recordStringBuffer.append("\n");
                    theByteArray = recordStringBuffer.toString().getBytes();
                    outputStream.write(theByteArray);

                    LOG.info("DBSource writing record to stream---> "+ recordStringBuffer);
                    recordCount++;
                    recordStringBuffer.setLength(0);
                }
                outputStream.close();
                LOG.info("DBSource finished processing" + recordCount);

            } catch (java.sql.SQLException e) {
                System.err.println(e);
                e.printStackTrace();
            }

            LOG.info("DBSource processed all records from table; Records written to output stream: " + recordCount);

        }
        catch (Exception e) {
            System.err.println (e);
            e.printStackTrace();
        }
        //cleanup
        finally {

            try {
                if (stmt != null) {
                    stmt.close();
                }

            }
            catch (java.sql.SQLException e) {
                System.err.println (e);
                e.printStackTrace();
            }

        }
    }
    public int getRecordCountInTable() throws IOException {

        try {

            LOG.info("DBSource.getRecordCountInTable starting " + recordCount);
            try {

                String query = "SELECT count(*) as record_count FROM ham_call_signs";
                // execute query
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(query);

                // return query result
                while (rs.next()) {

                    tableRecordCount = Integer.parseInt(rs.getString("record_count"));

                }
                LOG.info("DBSource.getRecordCountInTable finished processing " + recordCount);

            } catch (java.sql.SQLException e) {
                System.err.println(e);
                e.printStackTrace();
            }

        }
        catch (Exception e) {
            System.err.println (e);
            e.printStackTrace();
        }
        //cleanup
        finally {

            try {
                if (stmt != null) {
                    stmt.close();
                }

            }
            catch (java.sql.SQLException e) {
                System.err.println (e);
                e.printStackTrace();
            }
            return this.tableRecordCount;
        }
    }
    public int GetQueryResultCount() {
        return this.recordCount;
    }

}