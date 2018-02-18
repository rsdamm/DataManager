package com.plesba.datamanager.source;


import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;
import java.util.UUID;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharacterCodingException;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class KinesisSource {

    private static final String DEFAULT_APP_NAME = "KinesisConsumerDefault";
    private static final String DEFAULT_STREAMNAME = "KinesisLoaderDefault";
    private static final String DEFAULT_KINESIS_ENDPOINT = "https://kinesis.us-xxxx-1.amazonaws.com";
    private static final InitialPositionInStream DEFAULT_INITIAL_POSITION = InitialPositionInStream.LATEST; // Position can be one of LATEST (most recent data) or TRIM_HORIZON (oldest available data)

    private static String applicationName = DEFAULT_APP_NAME;
    private static String streamName = DEFAULT_STREAMNAME;
    private static String kinesisEndpoint = DEFAULT_KINESIS_ENDPOINT;
    private static InitialPositionInStream initialPositionInStream = DEFAULT_INITIAL_POSITION;
    private static String redisEndpoint = DEFAULT_KINESIS_ENDPOINT;
    private static int redisPort = 6379;

    private static KinesisClientLibConfiguration kinesisClientLibConfiguration;
    private static AWSCredentialsProvider credentialsProvider = null;
    private static boolean refresh = true;

    private static final Log LOG = LogFactory.getLog(KConsumer.class);

    private KinesisReader(Properties parameterProperties, PipedOutputStream oStream ) {

        loadProperties(parameterProperties);
        configure();


    }

    private static void configure(String propertiesFile) throws IOException {


        // ensure the JVM will refresh the cached IP values of AWS resources (e.g. service endpoints).
        java.security.Security.setProperty("networkaddress.cache.ttl", "60");
        LOG.info("Using workerId: " + workerId);

        // Get credentials from IMDS. If unsuccessful, get them from the credential profiles file.
        AWSCredentialsProvider credentialsProvider = null;
        try {
            LOG.info("trying default aws properties for credentials");
            credentialsProvider = new ProfileCredentialsProvider();
            // Verify we can fetch credentials from the provider
            AWSCredentials credentials = credentialsProvider.getCredentials();
            LOG.info("Obtained credentials from aws defaults.");
        } catch (Exception e) {
            LOG.info("Unable to obtain credentials default aws properties", e);
            credentialsProvider = null;
        }

        if (credentialsProvider == null) {
            try {

                credentialsProvider = new InstanceProfileCredentialsProvider(refresh);
                // Verify we can fetch credentials from the provider
                credentialsProvider.getCredentials();
                LOG.info("Obtained credentials from the IMDS.");
            } catch (AmazonClientException e) {
                LOG.info("Unable to obtain credentials from the IMDS", e);
            }
        }

        LOG.info("Using credentials with access key id: " + credentialsProvider.getCredentials().getAWSAccessKeyId());

        kinesisClientLibConfiguration = new KinesisClientLibConfiguration(applicationName, streamName,
                credentialsProvider, workerId).withInitialPositionInStream(initialPositionInStream); //.withRegionName(kinesisEndpoint);
    }


    private static void loadProperties(String propertiesFile) throws IOException {

        System.out.print("Loading properties from file: " + propertiesFile);

        FileInputStream inputStream = new FileInputStream(propertiesFile);
        Properties properties = new Properties();
        try {
            properties.load(inputStream);
        } finally {
            inputStream.close();
        }

        String appNameOverride = properties.getProperty(ConfigKeys.APPLICATION_NAME_KEY);
        if (appNameOverride != null) {
            applicationName = appNameOverride;
        }
        LOG.info("Using application name " + applicationName);

        String streamNameOverride = properties.getProperty(ConfigKeys.STREAM_NAME_KEY);
        if (streamNameOverride != null) {
            streamName = streamNameOverride;
        }
        LOG.info("Using stream name " + streamName);

        String kinesisEndpointOverride = properties.getProperty(ConfigKeys.KINESIS_ENDPOINT_KEY);
        if (kinesisEndpointOverride != null) {
            kinesisEndpoint = kinesisEndpointOverride;
        }
        LOG.info("Using Kinesis endpoint " + kinesisEndpoint);

        String initialPositionOverride = properties.getProperty(ConfigKeys.INITIAL_POSITION_IN_STREAM_KEY);
        if (initialPositionOverride != null) {
            initialPositionInStream = InitialPositionInStream.valueOf(initialPositionOverride);
        }
        LOG.info("Using initial position " + initialPositionInStream.toString() + " (if a checkpoint is not found).");

        String redisEndpointOverride = properties.getProperty(ConfigKeys.REDIS_ENDPOINT_KEY);
        if (redisEndpointOverride != null) {
            redisEndpoint = redisEndpointOverride;
        }
        LOG.info("Using Redis endpoint " + redisEndpoint);

        String redisPortOverride = properties.getProperty(ConfigKeys.REDIS_PORT_KEY);
        if (redisPortOverride != null) {
            try {
                redisPort = Integer.parseInt(redisPortOverride);
            } catch (NumberFormatException e) {

            }
        }
        LOG.info("Using Redis port " + redisPort);

    }
    public void processDatafromStream() {

        IRecordProcessorFactory recordProcessorFactory = new KCRecordProcessorFactory();
        Worker worker = new Worker(recordProcessorFactory, kinesisClientLibConfiguration);
        worker.run();

    }

    private class KCRecordProcessor implements IRecordProcessor {

        private static final Log LOG = LogFactory.getLog(KCRecordProcessor.class);
        private String kinesisShardId;

        // Backoff and retry settings
        private static final long BACKOFF_TIME_IN_MILLIS = 3000L;
        private static final int NUM_RETRIES = 10;

        // Checkpoint about once a minute
        private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L;
        private long nextCheckpointTimeInMillis;

        private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();


        private kcrecordprocessor() {
            super();
        }

        public void initialize(String shardId) {
            LOG.info("Initializing record processor for shard: " + shardId);
            this.kinesisShardId = shardId;
        }


        public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
            LOG.info("Processing " + records.size() + " records from " + kinesisShardId);

            // Process records and perform all exception handling.
            processRecordsWithRetries(records);

            // Checkpoint once every checkpoint interval.
            if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
                checkpoint(checkpointer);
                nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
            }

        }


        private void processRecordsWithRetries(List<Record> records) {
            for (Record record : records) {
                boolean processedSuccessfully = false;
                String data = null;
                for (int i = 0; i < NUM_RETRIES; i++) {
                    try {
                        // For this app, we interpret the payload as UTF-8 chars.
                        data = decoder.decode(record.getData()).toString();
                        LOG.info(record.getSequenceNumber() + ", " + record.getPartitionKey() + ", " + data);
                        //
                        // Logic to process record goes here.
                        //
                        processedSuccessfully = true;
                        break;
                    } catch (CharacterCodingException e) {
                        LOG.error("Malformed data: " + data, e);
                        break;
                    } catch (Throwable t) {
                        LOG.warn("Caught throwable while processing record " + record, t);
                    }

                    // backoff if we encounter an exception.
                    try {
                        Thread.sleep(BACKOFF_TIME_IN_MILLIS);
                    } catch (InterruptedException e) {
                        LOG.debug("Interrupted sleep", e);
                    }
                }

                if (!processedSuccessfully) {
                    LOG.error("Couldn't process record " + record + ". Skipping the record.");
                }
            }

            private void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
                LOG.info("Shutting down record processor for shard: " + kinesisShardId);
                // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
                if (reason == ShutdownReason.TERMINATE) {
                    checkpoint(checkpointer);
                }
            }

            private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
                LOG.info("Checkpointing shard " + kinesisShardId);
                for (int i = 0; i < NUM_RETRIES; i++) {
                    try {
                        checkpointer.checkpoint();
                        break;
                    } catch (ShutdownException se) {
                        // Ignore checkpoint if the processor instance has been shutdown (fail over).
                        LOG.info("Caught shutdown exception, skipping checkpoint.", se);
                        break;
                    } catch (ThrottlingException e) {
                        // Backoff and re-attempt checkpoint upon transient failures
                        if (i >= (NUM_RETRIES - 1)) {
                            LOG.error("Checkpoint failed after " + (i + 1) + "attempts.", e);
                            break;
                        } else {
                            LOG.info("Transient issue when checkpointing - attempt " + (i + 1) + " of "
                                    + NUM_RETRIES, e);
                        }
                    } catch (InvalidStateException e) {
                        // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
                        LOG.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
                        break;
                    }
                    try {
                        Thread.sleep(BACKOFF_TIME_IN_MILLIS);
                    } catch (InterruptedException e) {
                        LOG.debug("Interrupted sleep", e);
                    }
                }
            }

            private class KCRecordProcessorFactory implements IRecordProcessorFactory {

                public KCRecordProcessorFactory() {
                    super();
                }

                public IRecordProcessor createProcessor() {
                    return new KCRecordProcessor();
                }
            }

        }
