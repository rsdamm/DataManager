package com.plesba.datamanager.target;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.producer.*;
import com.amazonaws.services.kinesis.model.*;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
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
    private static AmazonKinesisClient kinesis;
    private KinesisProducerConfiguration config;
    List<Future<UserRecordResult>> putFutures = null;
    private KinesisProducer kinesisProducer = null;

    private int recordCount;


    private static final int DEFAULT_STREAMSIZE = 2;
    private static final String DEFAULT_STREAMNAME = "KinesisLoaderDefault";
    private static final String DEFAULT_KINESIS_REGION = "us-east-1";
    private static final String DEFAULT_PARTITION_KEY = "defaultPartitionKey";

    private static Integer streamSize = DEFAULT_STREAMSIZE;
    private static String streamName = DEFAULT_STREAMNAME;
    private static String kinesisRegion = DEFAULT_KINESIS_REGION;
    private static String partitionKeyName = DEFAULT_PARTITION_KEY;

    private static final Log LOG = LogFactory.getLog(KinesisTarget.class);

    public KinesisTarget(Properties parameterProperties, PipedInputStream parameterInputStream) throws InterruptedException {

        inputStream = parameterInputStream;
        recordCount = 0;
        describeStreamRequest = null;

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

        initializeStream(streamName);
    }

    public int GetLoadedCount() {
        return this.recordCount;
    }

    public void initializeStream(String streamName) throws InterruptedException {

        AmazonKinesisClientBuilder clientBuilder = AmazonKinesisClientBuilder.standard();
        //            clientBuilder.setRegion(regionName);
        //             clientBuilder.setCredentials(credentialsProvider);
        //             clientBuilder.setClientConfiguration(config);

        AmazonKinesis kinesis = clientBuilder.build();

        //create stream if it doesn't exist
        describeStreamRequest = new DescribeStreamRequest().withStreamName(streamName);
        try {
            StreamDescription streamDescription = kinesis.describeStream(describeStreamRequest).getStreamDescription();
            System.out.printf("KinesisTarget stream %s has a status of %s.\n", streamName, streamDescription.getStreamStatus());

            if ("DELETING".equals(streamDescription.getStreamStatus())) {
                System.out.println("KinesisTarget stream is being deleted. Processing terminating: " + streamName);
                System.exit(0);
            }

            // Wait for the stream to become active if it is not yet ACTIVE.
            if (!"ACTIVE".equals(streamDescription.getStreamStatus())) {
                waitForStreamToBecomeAvailable(streamName);
            }
        } catch (ResourceNotFoundException ex) {
            System.out.printf("KinesisTarget stream %s does not exist. Creating it now.\n", streamName);

            // Create a stream. The number of shards determines the provisioned throughput.
            CreateStreamRequest createStreamRequest = new CreateStreamRequest();
            createStreamRequest.setStreamName(streamName);
            createStreamRequest.setShardCount(streamSize);
            kinesis.createStream(createStreamRequest);
            // The stream is now being created. Wait for it to become active.
            waitForStreamToBecomeAvailable(streamName);
        }
        LOG.info("KinesisTarget stream initialized and ACTIVE ----------->" + streamName);

    }

    private static void waitForStreamToBecomeAvailable(String streamName) throws InterruptedException {
        System.out.printf("KinesisTarget waiting for %s to become ACTIVE...\n", streamName);

        long startTime = System.currentTimeMillis();
        long endTime = startTime + TimeUnit.MINUTES.toMillis(10);
        while (System.currentTimeMillis() < endTime) {
            Thread.sleep(TimeUnit.SECONDS.toMillis(20));

            try {
                DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
                describeStreamRequest.setStreamName(streamName);
                // ask for no more than 10 shards at a time -- this is an optional parameter
                describeStreamRequest.setLimit(10);
                DescribeStreamResult describeStreamResponse = kinesis.describeStream(describeStreamRequest);

                String streamStatus = describeStreamResponse.getStreamDescription().getStreamStatus();
                System.out.printf("\t- KinesisTarget current state: %s\n", streamStatus);
                if ("ACTIVE".equals(streamStatus)) {
                    return;
                }
            } catch (ResourceNotFoundException ex) {
                // ResourceNotFound means the stream doesn't exist yet,
                // so ignore this error and just keep polling.
            } catch (AmazonServiceException ase) {
                throw ase;
            }
        }

        throw new RuntimeException(String.format("KinesisTarget stream %s never became active", streamName));
    }

    public void processDataFromInputStream() {

        System.out.println("KinesisTarget started processing ....");
        StringBuilder recordStringBuffer = new StringBuilder();
        String streamRecord = new String();
        kinesisProducer = new KinesisProducer();

        putFutures = new LinkedList<Future<UserRecordResult>>();

        try {

            int streamByte = inputStream.read();

            while (streamByte != -1) { //end of stream
                if (streamByte != 10) {  //end of line
                    recordStringBuffer.append((char) streamByte);
                } else { // process record
                    streamRecord = recordStringBuffer.toString() + '\n';
                    System.out.println("Record to put placed on stream: " + streamRecord);
                    try {
                      //  putFutures.add(kinesisProducer.addUserRecord(streamName, partitionKeyName, ByteBuffer.wrap(streamRecord.getBytes("UTF-8"))));
                         kinesisProducer.addUserRecord(streamName, partitionKeyName, ByteBuffer.wrap(streamRecord.getBytes("UTF-8")));
                    } catch (UnsupportedEncodingException ex) {
                        Logger.getLogger(KinesisTarget.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    recordCount = recordCount + 1;
                    recordStringBuffer.setLength(0);

                    System.out.println("KinesisTarget record: " + recordCount);
                }
                streamByte = inputStream.read();
                //lines.close();
            }

         //   System.out.println("KinesisTarget Checking futures result");
         //   for (Future<UserRecordResult> f : putFutures) {
         //       UserRecordResult result = f.get(); // this does block
         //       if (result.isSuccessful()) {
         //           System.out.println("KinesisTarget Put record into shard " + result.getShardId());
         //       } else {
         //           for (Attempt attempt : result.getAttempts()) {
         //               // Analyze and respond to the failure
         //               if (attempt.getErrorMessage() != null) {
         //                   String errorMessages = attempt.getErrorMessage() + "\n";
         //                   System.out.println("KinesisTarget error: " + errorMessages);
         //               }
         //           }
         //       }
         //   }


     //       stop();

        } catch(Exception ex){
            Logger.getLogger(KinesisTarget.class.getName()).log(Level.SEVERE, null, ex);
        }

        System.out.println("KinesisTarget (Producer) finished processing........." );
    }
    public void stop(){

        kinesisProducer.flushSync();
        System.out.println("KinesisTarget (Producer) flushSync completed");
        kinesisProducer.destroy();

        System.out.println("KinesisTarget (Producer) destroy completed");

    }

}
