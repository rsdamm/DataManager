package com.plesba.datamanager.source; 

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.PipedOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

public class DBSource {

    private final Connection connection;
    private final PipedOutputStream outputStream;
    private final String insert_dml = "INSERT INTO ham_call_signs (NAME, CALL_SIGN) VALUES (?, ?)";
    private PreparedStatement stmt = null;
    private int recordCount = 0;
    private StringBuilder recordStringBuffer=null;
    private String dbRecord = null;
    private byte[] theByteArray = null;
    private List<String> cols ;
    private String [] nextLine = null;
    private int i=0;
    private String rowName = null;
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
                    System.out.println("DBSource query result: " + rowName + " - " + rowCallSign);

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
        //close the connection
        finally {

            try {
                if (stmt != null) {
                    stmt.close();
                }

                if (connection != null) {
                    connection.close();
                    LOG.info("DBSource completed.");
                }
            }
            catch (java.sql.SQLException e) {
                System.err.println (e);
                e.printStackTrace();
            }

        }
    }

    public int GetQueryResultCount() {
        return this.recordCount;
    }

}
