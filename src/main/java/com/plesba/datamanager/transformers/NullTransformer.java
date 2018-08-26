package com.plesba.datamanager.transformers;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

public class NullTransformer {
    private final PipedInputStream inputStream;
    private PipedOutputStream outputStream;
    private int recordCount = 0;
    private byte[] theByteArray = null;
    private static final Log LOG = LogFactory.getLog(NullTransformer.class);

    public NullTransformer(PipedInputStream parameterInputStream, PipedOutputStream parameterOutputStream ) {

        LOG.info("NullTransformer started processing");
        inputStream = parameterInputStream;
        outputStream = parameterOutputStream;
    }

    public NullTransformer() {

        LOG.info("NullTransformer started processing with no parameters");
        inputStream = null;
        outputStream = null;
    }

    public void processDataFromInputStream() throws IOException {

        try {
            LOG.info("NullTransformer reading from input stream/writing to output stream");

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

                    //write to output stream
                    recordStringBuffer.append("\n");
                    theByteArray = recordStringBuffer.toString().getBytes();
                    outputStream.write(theByteArray);

                    LOG.info("NullTransformer processed record: " + streamRecord);
                    recordStringBuffer.setLength(0);
                }
                streamByte = inputStream.read();

            }
            outputStream.close();
            LOG.info(String.format("NullTransformer finished processing - records processed: %d", recordCount));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public int getTransformedCount() {
        return this.recordCount;
    }
}
