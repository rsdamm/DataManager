package com.plesba.datamanager.target;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.amazonaws.services.kinesis.model.*;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class KinesisProducer {

        private final String outStreamName;
        private final PipedInputStream inputStream;
        private int recordCount;
        Integer myStreamSize;
        DescribeStreamRequest describeStreamRequest ;
        List<Future<UserRecordResult>> putFutures = null;
        private static AmazonKinesisClient kinesis;

        public KinesisProducer(String parameterStreamName, PipedInputStream parameterInputStream) throws InterruptedException {
            outStreamName = parameterStreamName;
            recordCount = 0;
            describeStreamRequest = null;
            myStreamSize=1;
            initializeStream(outStreamName);
        }

        public int GetLoadedCount() {
            return this.recordCount;
        }
        public void initializeStream(String kStreamName) throws InterruptedException {

            AmazonKinesisClientBuilder clientBuilder = AmazonKinesisClientBuilder.standard();
            //            clientBuilder.setRegion(regionName);
            //             clientBuilder.setCredentials(credentialsProvider);
            //             clientBuilder.setClientConfiguration(config);

            AmazonKinesis kinesis = clientBuilder.build();

            //create stream if it doesn't exist
            describeStreamRequest = new DescribeStreamRequest().withStreamName(outStreamName);
            try {
                StreamDescription streamDescription = kinesis.describeStream(describeStreamRequest).getStreamDescription();
                System.out.printf("Stream %s has a status of %s.\n", outStreamName, streamDescription.getStreamStatus());

                if ("DELETING".equals(streamDescription.getStreamStatus())) {
                    System.out.println("Stream is being deleted. Processing terminating.");
                    System.exit(0);
                }

                // Wait for the stream to become active if it is not yet ACTIVE.
                if (!"ACTIVE".equals(streamDescription.getStreamStatus())) {
                    waitForStreamToBecomeAvailable(kStreamName);
                }
            } catch (ResourceNotFoundException ex) {
                System.out.printf("Stream %s does not exist. Creating it now.\n", kStreamName);

                // Create a stream. The number of shards determines the provisioned throughput.
                CreateStreamRequest createStreamRequest = new CreateStreamRequest();
                createStreamRequest.setStreamName(kStreamName);
                createStreamRequest.setShardCount(myStreamSize);
                kinesis.createStream(createStreamRequest);
                // The stream is now being created. Wait for it to become active.
                waitForStreamToBecomeAvailable(kStreamName);
            }


        }
        private static void waitForStreamToBecomeAvailable(String myStreamName) throws InterruptedException {
            System.out.printf("Waiting for %s to become ACTIVE...\n", myStreamName);

            long startTime = System.currentTimeMillis();
            long endTime = startTime + TimeUnit.MINUTES.toMillis(10);
            while (System.currentTimeMillis() < endTime) {
                Thread.sleep(TimeUnit.SECONDS.toMillis(20));

                try {
                    DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
                    describeStreamRequest.setStreamName(myStreamName);
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

            throw new RuntimeException(String.format("Stream %s never became active", myStreamName));
        }
        public void processDataFromInputStream() {

            KinesisProducerConfiguration config = KinesisProducerConfiguration.fromPropertiesFile(configFileName);
            final KinesisProducer kinesisProducer;
            try {
                kinesisProducer = new KinesisProducer(config);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }


            try {
                putFutures = new LinkedList<Future<UserRecordResult>>();
                Stream<String> lines = Files.lines(Paths.get(loadFileName));
                lines.forEach(
                        s -> {
                            try {
                                putFutures.add(kinesisProducer.addUserRecord(theStreamName, "partitionKey1", ByteBuffer.wrap(s.getBytes("UTF-8"))));
                            } catch (UnsupportedEncodingException ex) {
                                Logger.getLogger(KinesisProducer.class.getName()).log(Level.SEVERE, null, ex);
                            }
                            recordCount = recordCount + 1;
                        }
                );

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
            } catch (UnsupportedEncodingException ex) {
                Logger.getLogger(KinesisProducer.class.getName()).log(Level.SEVERE, null, ex);
            } catch (InterruptedException ex) {
                Logger.getLogger(KinesisProducer.class.getName()).log(Level.SEVERE, null, ex);
            } catch (ExecutionException ex) {
                Logger.getLogger(KinesisProducer.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException e) {
                System.err.println(e);
                Logger.getLogger(KinesisProducer.class.getName()).log(Level.SEVERE, null, e);
            }
        }

    }}
