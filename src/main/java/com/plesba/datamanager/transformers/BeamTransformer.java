package com.plesba.datamanager.transformers;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class BeamTransformer {
    private PipedInputStream inputStream;
    private PipedOutputStream outputStream;
    private int recordCount = 0;
    private int recordCountO = 0;
    private byte[] theByteArray = null;
    private static final Log LOG = LogFactory.getLog(BeamTransformer.class);
    
    private List<String> stringList = null;
    //private ArrayList<String>  inputStringList = new ArrayList<String>();

    // reads input stream; creates a Beam pipeline, transforms; writes to output stream
    public BeamTransformer(PipedInputStream parameterInputStream, PipedOutputStream parameterOutputStream ) {

        LOG.info("BeamTransformer started processing");
        inputStream = parameterInputStream;
        outputStream = parameterOutputStream;
    }

    public void processDataFromInputStream() throws IOException {

     /*   try {
            LOG.info("BeamTransformer reading from in-memory data (input stream)/writing to output stream");
            //using Direct Runner as default
            PipelineOptions options = PipelineOptionsFactory.create();
            Pipeline p = Pipeline.create(options);

            StringBuilder recordStringBuffer = new StringBuilder();
            String streamRecord = new String();
            //Read one line at a time

            int streamByte = inputStream.read();

            while (streamByte != -1) {
                /* until end of stream */
           /*     if (streamByte != 10) {
                    //end of line
                    recordStringBuffer.append((char) streamByte);
                } else {
                    /* process record */
             /*       recordCount++;
                    streamRecord = recordStringBuffer.toString() + '\n';
                    
                    //send resulting record to be gathered into an array of strings
                    stringList.add(streamRecord);
                    
                    LOG.info("BeamTransformer received record: " + streamRecord);
                    recordStringBuffer.setLength(0);
                }
                streamByte = inputStream.read();

            }
            LOG.info("BeamTransformer read " + recordCount + " records");
*/
            //normally this would be an external source (file, db, etc) converted in the apply
            // but here using in-memory with input/output stream

            //build the collection and pass to transformer

            //create input collection
  //          PCollection<String> stringInPCollection = p.apply(Create.of(stringList)).setCoder(StringUtf8Coder.of());

    //        LOG.info("BeamTransformer input collection created ");

            //define the transformation and create output collection
      //      PCollection<String> stringOutPCollection = stringInPCollection.apply(Count.<String>perElement());

            //run the transformation
   /*         p.run();

            LOG.info("BeamTransformer transformation performed; output collection created ");

            //unpack the resulting collection and put data on output stream
      //      for (Iterator i = stringOutPCollection.iterator(); i.hasNext();) {

                    recordCountO++;

     //               streamRecord = i.next;

                    //send resulting record to outputstream

                    recordStringBuffer.append("\n");
                    theByteArray = recordStringBuffer.toString().getBytes();
                    outputStream.write(theByteArray);
                    //

                    LOG.info("BeamTransformer output record: " + streamRecord);
                    recordStringBuffer.setLength(0);

            }

            outputStream.close();
            LOG.info(String.format("BeamTransformer finished processing - records processed: %d", recordCountO));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public int getTransformedCount() {
        return this.recordCount;
        */
    }

}
