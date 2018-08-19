/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.plesba.datamanager.target;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.FileWriter;
import java.sql.*;
import java.util.Arrays;
import java.util.List;

/**
 * @author renee
 * <p>
 * Writes to csv file
 */
public class CSVTarget {

    private final PipedInputStream inputStream;
    private int recordCount = 0;
    private FileWriter fileWriter;
    private String outFile;

    private static final Log LOG = LogFactory.getLog(CSVTarget.class);

    public CSVTarget(String parameterOutFilename, PipedInputStream parameterInputStream) {

        LOG.info("CSVTarget started processing");
        inputStream = parameterInputStream;
        outFile = parameterOutFilename;

        LOG.info("CSVTarget outputFile as set in constructor " + outFile);
    }

    public CSVTarget() {

        LOG.info("CSVTarget started processing with no parameters");
        inputStream = null;
        fileWriter = null;
    }

    public void processDataFromInputStream() throws IOException {

        try {
            LOG.info("CSVTarget writing to " + outFile);
            fileWriter = fileWriter = new FileWriter(outFile);

            StringBuilder recordStringBuffer = new StringBuilder();
            String streamRecord = new String();
            //Read one line at a time

            int streamByte = inputStream.read();

            while (streamByte != -1) {
                /* until end of stream */
                if (streamByte != 10) {
                    //end of line
                    recordStringBuffer.append((char) streamByte);
                } else {
                    /* process record */
                    recordCount++;
                    streamRecord = recordStringBuffer.toString() + '\n';
                    fileWriter.append(streamRecord);

                    LOG.info("CSVTarget Processed record: " + streamRecord);
                    recordStringBuffer.setLength(0);
                }
                streamByte = inputStream.read();

            }

            LOG.info("CSVTarget all records processed.");

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {

                fileWriter.flush();
                fileWriter.close();
                LOG.info(String.format("CSVTarget finished processing - records processed: %d", recordCount));

            } catch (IOException e) {

            }
        }
    }

    public int GetLoadedCount() {
        return this.recordCount;
    }

}
