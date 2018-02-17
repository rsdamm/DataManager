package com.plesba.datamanager.target;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
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

public class KinesisWriter {

    private final PipedInputStream inputStream;

    private final String streamName;
    private final String initialPosition;
    private final String regionName;
    private final Integer streamSize;
    private final String partitionKeyName;

    private DescribeStreamRequest describeStreamRequest;
    List<Future<UserRecordResult>> putFutures = null;
    private static AmazonKinesisClient kinesis;
    private KinesisProducerConfiguration config;

    private int recordCount;


    public KinesisWriter(Properties parameterProperties, PipedInputStream parameterInputStream) throws InterruptedException {

        inputStream = parameterInputStream;
        recordCount = 0;
        describeStreamRequest = null;

        streamName = parameterProperties.getProperty("kinesis.streamname");
        initialPosition = parameterProperties.getProperty("kinesis.initialpositioninstream");
        streamSize = Integer.parseInt(parameterProperties.getProperty("kinesis.streamsize"));
        regionName = parameterProperties.getProperty("kinesis.region");
        partitionKeyName = parameterProperties.getProperty("kinesis.partitionKey");

        KinesisProducerConfiguration kpconfig = new KinesisProducerConfiguration()
                .setRegion(regionName);
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
            System.out.printf("Stream %s has a status of %s.\n", streamName, streamDescription.getStreamStatus());

            if ("DELETING".equals(streamDescription.getStreamStatus())) {
                System.out.println("Stream is being deleted. Processing terminating.");
                System.exit(0);
            }

            // Wait for the stream to become active if it is not yet ACTIVE.
            if (!"ACTIVE".equals(streamDescription.getStreamStatus())) {
                waitForStreamToBecomeAvailable(streamName);
            }
        } catch (ResourceNotFoundException ex) {
            System.out.printf("Stream %s does not exist. Creating it now.\n", streamName);

            // Create a stream. The number of shards determines the provisioned throughput.
            CreateStreamRequest createStreamRequest = new CreateStreamRequest();
            createStreamRequest.setStreamName(streamName);
            createStreamRequest.setShardCount(streamSize);
            kinesis.createStream(createStreamRequest);
            // The stream is now being created. Wait for it to become active.
            waitForStreamToBecomeAvailable(streamName);
        }

    }

    private static void waitForStreamToBecomeAvailable(String streamName) throws InterruptedException {
        System.out.printf("Waiting for %s to become ACTIVE...\n", streamName);

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
                System.out.printf("\t- current state: %s\n", streamStatus);
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

        throw new RuntimeException(String.format("Stream %s never became active", streamName));
    }

    public void processDataFromInputStream() {

        StringBuilder recordStringBuffer = new StringBuilder();
        String streamRecord = new String();

        final KinesisProducer kinesisProducer = new KinesisProducer();



        try {

            int streamByte = inputStream.read();
            putFutures = new LinkedList<Future<UserRecordResult>>();

            while (streamByte != -1) { //end of stream
                if (streamByte != 10) {  //end of line
                    recordStringBuffer.append((char) streamByte);
                } else { // process record
                    streamRecord = recordStringBuffer.toString() + '\n';
                    try {
                        putFutures.add(kinesisProducer.addUserRecord(streamName, partitionKeyName, ByteBuffer.wrap(streamRecord.getBytes("UTF-8"))));
                    } catch (UnsupportedEncodingException ex) {
                        Logger.getLogger(KinesisWriter.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    recordCount = recordCount + 1;
                    streamByte = inputStream.read();
                }

                //lines.close();
                System.out.println("Attempted records: " + recordCount);

                for (Future<UserRecordResult> f : putFutures) {
                    System.out.println("Checking result for " + f.toString());
                    UserRecordResult result = f.get(); // this does block
                    if (result.isSuccessful()) {
                        System.out.println("Put record into shard "
                                + result.getShardId());
                    } else {
                        for (Attempt attempt : result.getAttempts()) {
                            // Analyze and respond to the failure
                        }
                    }
                }
                kinesisProducer.flushSync();
                //getRecords();
            }
        } catch(UnsupportedEncodingException ex){
                Logger.getLogger(KinesisWriter.class.getName()).log(Level.SEVERE, null, ex);
        } catch(InterruptedException ex){
                Logger.getLogger(KinesisWriter.class.getName()).log(Level.SEVERE, null, ex);
        }catch(IOException ex){
            Logger.getLogger(KinesisWriter.class.getName()).log(Level.SEVERE, null, ex);
        }catch(ExecutionException ex){
            Logger.getLogger(KinesisWriter.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
