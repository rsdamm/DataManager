/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.plesba.datamanager.target;

import java.io.File;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.FileWriter;
import java.sql.*;
import java.util.Arrays;
import java.util.List;

/**
 *
 * @author renee
 *
 *   Writes to csv file
 */
public class CSVWriter {

    private final PipedInputStream inputStream;
    private int recordCount = 0;
    private FileWriter fileWriter;
    private String outFile;

    public CSVWriter( String parameterOutFilename, PipedInputStream parameterInputStream) {
        inputStream = parameterInputStream;
        outFile = parameterOutFilename;
    }

    public CSVWriter() {
        inputStream = null;
        fileWriter = null;
    }

    public void processDataFromInputStream() throws IOException {

        try {

            fileWriter = fileWriter = new FileWriter(outFile);

            StringBuilder recordStringBuffer = new StringBuilder();
            String streamRecord = new String();
            //Read one line at a time

            int streamByte = inputStream.read();

            while (streamByte != -1) {  //end of stream

                if (streamByte != 10) { //end of line
                    recordStringBuffer.append((char) streamByte);
                } else { //process record
                    recordCount++;
                    streamRecord = recordStringBuffer.toString() + '\n';
                    fileWriter.append(streamRecord);
                    System.out.println("Processed record: " + streamRecord);
                    recordStringBuffer.setLength(0);
                }
                streamByte = inputStream.read();

            }
            System.out.println("Wrote records to csv: " + recordCount);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {

                fileWriter.flush();
                fileWriter.close();

            } catch (IOException e) {

            }
        }
    }

    public int GetLoadedCount() {
        return this.recordCount;
    }

}
