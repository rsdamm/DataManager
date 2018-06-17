package com.plesba.datamanager.target;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.*;

import java.io.PipedInputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


//read input stream; write to kinesis stream (producer)
public class KinesisTarget {

    private final PipedInputStream inputStream;
    private DescribeStreamRequest describeStreamRequest;
    private static AmazonKinesis kinesis= null;
    private AmazonKinesisClientBuilder clientBuilder = null;
    private StreamDescription streamDescription = null;
    private CreateStreamRequest createStreamRequest = null;
    private DescribeStreamResult describeStreamResult = null;
    private int recordCount =0;
    private String streamStatus= null;


    private static final int DEFAULT_STREAMSIZE = 2;
    private static final String DEFAULT_STREAMNAME = "KinesisLoaderDefault";
    private static final String DEFAULT_KINESIS_REGION = "us-east-1";
    private static final String DEFAULT_PARTITION_KEY = "defaultPartitionKey";
    private static final int DEFAULT_MAX_RECORDS_TO_PROCESS = -1;

    private static Integer streamSize = DEFAULT_STREAMSIZE;
    private static String streamName = DEFAULT_STREAMNAME;
    private static String kinesisRegion = DEFAULT_KINESIS_REGION;
    private static String partitionKeyName = DEFAULT_PARTITION_KEY;
    private static Integer maxRecordsToProcess = DEFAULT_MAX_RECORDS_TO_PROCESS;

    private boolean stopProcessing=false;
    private static final Log LOG = LogFactory.getLog(KinesisTarget.class);

    public KinesisTarget(Properties parameterProperties, PipedInputStream parameterInputStream) throws InterruptedException {

        inputStream = parameterInputStream;


        String streamNameOverride = parameterProperties.getProperty("kinesis.streamname");
        if (streamNameOverride != null) {
            streamName = streamNameOverride;
        }
        LOG.info("KinesisTarget using stream name " + streamName);

        String streamSizeOverride = parameterProperties.getProperty("kinesis.streamsize");
        if (streamSizeOverride != null) {
            streamSize = Integer.parseInt(streamSizeOverride);
        }
        LOG.info("KinesisTarget using stream size " + streamSize);

        String partitionKeyOverride = parameterProperties.getProperty("kinesis.partitionkey");
        if (partitionKeyOverride != null) {
            partitionKeyName = partitionKeyOverride;
        }
        LOG.info("KinesisTarget using Kinesis partitionkey " + partitionKeyName);

        String kinesisRegionOverride = parameterProperties.getProperty("kinesis.region");
        if (kinesisRegionOverride != null) {
            kinesisRegion = kinesisRegionOverride;
        }
        LOG.info("KinesisTarget using Kinesis region " + kinesisRegion);

        String maxRecordsToProcessOverride = parameterProperties.getProperty("kinesis.maxrecordstoprocess");
        if (maxRecordsToProcessOverride != null) {
            maxRecordsToProcess = Integer.parseInt(maxRecordsToProcessOverride);

        }
        LOG.info("KinesisTarget using maxRecordsToProcess " + maxRecordsToProcess);


        describeStreamRequest =  new DescribeStreamRequest().withStreamName(streamName);
        describeStreamRequest.setLimit(10);
        initializeStream(streamName);

    }


    public int GetLoadedCount() {
        return this.recordCount;
    }

    public void initializeStream(String streamName) throws InterruptedException {

        clientBuilder = AmazonKinesisClientBuilder.standard();
        //            clientBuilder.setRegion(regionName);
        //             clientBuilder.setCredentials(credentialsProvider);
        //             clientBuilder.setClientConfiguration(config);

        kinesis = clientBuilder.build();

        //create stream if it doesn't exist; exception generated if does not exist
        try {
            streamDescription = kinesis.describeStream(describeStreamRequest).getStreamDescription();

            LOG.info("\"KinesisTarget stream %s has a status of " + streamName + " "  +streamDescription.getStreamStatus());
            if ("DELETING".equals(streamDescription.getStreamStatus())) {

                LOG.info("KinesisTarget stream is being deleted. Processing terminating: " + streamName);
                System.exit(0);
            }

            // Wait for the stream to become active if it is not yet ACTIVE.
            if (!"ACTIVE".equals(streamDescription.getStreamStatus())) {
                waitForStreamToBecomeAvailable(streamName);
            }
        } catch (ResourceNotFoundException ex) {
            LOG.info("KinesisTarget stream does not exist. Creating it now." +streamName);

            // Create a stream. The number of shards determines the provisioned throughput.
            createStreamRequest = new CreateStreamRequest();
            createStreamRequest.setStreamName(streamName);
            createStreamRequest.setShardCount(streamSize);
            kinesis.createStream(createStreamRequest);
            // The stream is now being created. Wait for it to become active.
            waitForStreamToBecomeAvailable(streamName);
        }
        LOG.info("KinesisTarget stream initialized and ACTIVE ----------->" + streamName);

    }

    private void waitForStreamToBecomeAvailable(String streamName) throws InterruptedException {

        LOG.info("KinesisTarget waiting for stream to become ACTIVE." + streamName);
        long startTime = System.currentTimeMillis();
        long endTime = startTime + TimeUnit.MINUTES.toMillis(10);

        LOG.info("KinesisTarget waiting for to become ACTIVE..." + startTime);
        LOG.info("KinesisTarget waiting for to become ACTIVE..." + endTime);
        while (System.currentTimeMillis() < endTime) {

            try {
                describeStreamResult = kinesis.describeStream(describeStreamRequest);

                streamStatus = describeStreamResult.getStreamDescription().getStreamStatus();

                LOG.info("KinesisTarget current state " + streamStatus);
                if ("ACTIVE".equals(streamStatus)) {
                    return;
                }
            } catch (ResourceNotFoundException ex) {
                // ResourceNotFound means the stream doesn't exist yet,
                // so ignore this error and just keep polling.

                LOG.info("KinesisTarget still waiting for stream" + streamName + "  to become ACTIVE. Will fail at "+ endTime);
            } catch (AmazonServiceException ase) {
                throw ase;
            }
            Thread.sleep(TimeUnit.SECONDS.toMillis(20));
        }

        throw new RuntimeException(String.format("KinesisTarget stream %s never became active", streamName));
    }

    public void processDataFromInputStream() {

        LOG.info("KinesisTarget started stream processing " + streamName);

        StringBuilder recordStringBuffer = new StringBuilder();
        String streamRecord = new String();

        try {

            int streamByte = inputStream.read();

            while (streamByte != -1 & !stopProcessing) {   //end of stream
                if (streamByte != 10) {  //end of line
                    recordStringBuffer.append((char) streamByte);
                } else { // process record
                    streamRecord = recordStringBuffer.toString() + '\n';

                    LOG.info("KinesisTarget Record to put placed on stream: " + streamRecord);
                    try {
                         kinesis.putRecord(streamName,  ByteBuffer.wrap(streamRecord.getBytes("UTF-8")),partitionKeyName);
                    } catch (UnsupportedEncodingException ex) {
                        Logger.getLogger(KinesisTarget.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    recordCount = recordCount + 1;
                    recordStringBuffer.setLength(0);
                    if (recordCount >= maxRecordsToProcess & maxRecordsToProcess > -1 ){ stopProcessing = true;}

                    LOG.info("KinesisTarget records written to stream: " + recordCount);
                }


                streamByte = inputStream.read();
            }


            kinesis.putRecord(streamName,  ByteBuffer.wrap(streamRecord.getBytes("UTF-8")),partitionKeyName);
            LOG.info("KinesisTarget completed processed "+ recordCount + " records.");

        } catch(Exception ex){

            LOG.error("KinesisTarget error detected in processDataFromInputStream " ,ex);

        }

        LOG.info("KinesisTarget (Producer) finished processing");
    }


}
