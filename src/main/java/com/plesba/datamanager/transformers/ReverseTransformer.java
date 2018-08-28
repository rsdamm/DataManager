package com.plesba.datamanager.transformers;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

public class ReverseTransformer {
    private final PipedInputStream inputStream;
    private PipedOutputStream outputStream;
    private int recordCount = 0;
    private byte[] theByteArray = null;
    private static final Log LOG = LogFactory.getLog(ReverseTransformer.class);
    private String csvDelimiter = ",";

    public ReverseTransformer(PipedInputStream parameterInputStream, PipedOutputStream parameterOutputStream ) {

        LOG.info("NullTransformer started processing");
        inputStream = parameterInputStream;
        outputStream = parameterOutputStream;
    }

    public ReverseTransformer() {

        LOG.info("ReverseTransformer started processing with no parameters");
        inputStream = null;
        outputStream = null;

    }

    public void processDataFromInputStream() throws IOException {

        try {
            LOG.info("ReverseTransformer reading from input stream/writing to output stream");

            StringBuilder recordStringBuffer = new StringBuilder();
            String streamRecord = new String();
            //Read one line at a time

            int streamByte = inputStream.read();
            int j = 0;

            while (streamByte != -1) {
                /* until end of stream */
                if (streamByte != 10) {
                    //end of line
                    recordStringBuffer.append((char) streamByte);
                } else {
                    /* process record */
                    recordCount++;

                    //reverse the columns
                    streamRecord = recordStringBuffer.toString();
                    String[] arrayOfColumns = streamRecord.split(csvDelimiter);
                    String[] revData = new String[arrayOfColumns.length];
                    j=0;
                    for (int i = arrayOfColumns.length-1; i>=0 ;i--) {
                          revData[j]= arrayOfColumns[i];
                          j++;
                    }
                    streamRecord = String.join(",", revData) + '\n';

                    //write to output stream
                    theByteArray = streamRecord.getBytes();
                    outputStream.write(theByteArray);

                    LOG.info("ReverseTransformer processed record: " + streamRecord);
                    recordStringBuffer.setLength(0);
                }
                streamByte = inputStream.read();        

            }
            outputStream.close();
            LOG.info(String.format("ReverseTransformer finished processing - records processed: %d", recordCount));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public int getTransformedCount() {
        return this.recordCount;
    }
}
