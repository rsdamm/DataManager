/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.plesba.datapiper.target;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.FileWriter;

/**
 * @author renee
 * <p>
 * Writes to csv file
 */
public class CSVTargetFromStream {

    private PipedInputStream inputStream;
    private int recordCount = 0;
    private FileWriter fileWriter;
    private String outFile;

    private static final Log LOG = LogFactory.getLog(CSVTargetFromStream.class);

    public CSVTargetFromStream(String parameterOutFilename, PipedInputStream parameterInputStream) {

        LOG.info("CSVTargetFromStream started processing");
        inputStream = parameterInputStream;
        outFile = parameterOutFilename;

        LOG.info("CSVTargetFromStream outputFile: " + outFile);
    }

    public CSVTargetFromStream() {

        LOG.info("CSVTargetFromStream started processing with no parameters");
        inputStream = null;
        fileWriter = null;
    }

    public void processDataFromInputStream() throws IOException {
        LOG.info("CSVTargetFromStream processDataFromInputStream");
        try {
            LOG.info("CSVTargetFromStream writing to " + outFile);
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

                    LOG.info("CSVTargetFromStream Processed record: " + streamRecord);
                    recordStringBuffer.setLength(0);
                }
                streamByte = inputStream.read();

            }

            LOG.info("CSVTargetFromStream all records processed.");

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {

                fileWriter.flush();
                fileWriter.close();
                LOG.info(String.format("CSVTargetFromStream finished processing - records processed: %d", recordCount));

            } catch (IOException e) {

            }
        }
    }

    public int GetLoadedCount() {
        return this.recordCount;
    }

}
