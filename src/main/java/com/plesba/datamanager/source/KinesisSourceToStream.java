package com.plesba.datamanager.source;

import java.io.IOException;
import java.io.PipedOutputStream;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharacterCodingException;
import java.net.InetAddress;
import java.util.UUID;
import java.util.List;
import java.util.Properties;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
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

//reads kinesis stream writes to output stream
public class KinesisSourceToStream {

    private static final String DEFAULT_APP_NAME = "KinesisConsumerDefault";
    private static final int DEFAULT_STREAMSIZE = 2;
    private static final int DEFAULT_MAX_RECORDS_TO_PROCESS = -1;
    private static final String DEFAULT_STREAMNAME = "KinesisLoaderDefault";
    private static final String DEFAULT_KINESIS_ENDPOINT = "https://kinesis.us-xxxx-1.amazonaws.com";
    private static final String DEFAULT_KINESIS_REGION = "us-xxxx-1";
    private static final InitialPositionInStream DEFAULT_INITIAL_POSITION = InitialPositionInStream.LATEST; // Position can be one of LATEST (most recent data) or TRIM_HORIZON (oldest available data)
    private static final String DEFAULT_PARTITION_KEY = "defaultPartitionKey";

    private static String applicationName = DEFAULT_APP_NAME;
    private static int streamSize = DEFAULT_STREAMSIZE;
    private static String streamName = DEFAULT_STREAMNAME;
    private static String kinesisEndpoint = DEFAULT_KINESIS_ENDPOINT;
    private static String kinesisRegion = DEFAULT_KINESIS_REGION;
    private static InitialPositionInStream initPosInStream = DEFAULT_INITIAL_POSITION;
    private static String partitionKey = DEFAULT_PARTITION_KEY;
    private static Integer maxRecordsToProcess = DEFAULT_MAX_RECORDS_TO_PROCESS;

    private byte[] theByteArray = null;
    private StringBuilder recordStringBuffer;
    private int i=0;
    private String nextLine = null;
    private PipedOutputStream outputStream;
    private int recordCount = 0;
    private boolean stopProcessing=false;

    private static KinesisClientLibConfiguration kinesisClientLibConfiguration;
    private static AWSCredentialsProvider credentialsProvider = null;
    private static boolean refresh = true;

    private static final Log LOG = LogFactory.getLog(KinesisSourceToStream.class);

    public KinesisSourceToStream(Properties parameterProperties, PipedOutputStream parameterOutputStream) {
        recordStringBuffer = new StringBuilder();

        LOG.info("KinesisSourceToStream (consumer) started processing.");

        outputStream = parameterOutputStream;
        String appNameOverride = parameterProperties.getProperty("kinesis.applicationname");
        if (appNameOverride != null) {
            applicationName = appNameOverride;
        }

        LOG.info("KinesisSourceToStream (consumer) application name "+ applicationName);

        String streamNameOverride = parameterProperties.getProperty("kinesis.streamname");
        if (streamNameOverride != null) {
            streamName = streamNameOverride;
        }
        LOG.info("KinesisSourceToStream (consumer) stream name "+ streamName);

        String streamSizeOverride = parameterProperties.getProperty("kinesis.streamsize");
        if (streamSizeOverride != null) {
            streamSize = Integer.parseInt(streamSizeOverride);
        }

        LOG.info("KinesisSourceToStream (consumer) streamsize "+ streamSize);

        String initialPositionInStreamOverride = parameterProperties.getProperty("kinesis.initialpositioninstream");
        if (initialPositionInStreamOverride != null) {
            initPosInStream = InitialPositionInStream.valueOf(initialPositionInStreamOverride);
        }

        LOG.info("KinesisSourceToStream (consumer) initial position in stream "+ initPosInStream.toString());

        String kinesisEndpointOverride = parameterProperties.getProperty("kinesis.endpoint");
        if (kinesisEndpointOverride != null) {
            kinesisEndpoint = kinesisEndpointOverride;
        }
        LOG.info("KinesisSourceToStream (consumer) endpoint "+ kinesisEndpoint);

        String kinesisRegionOverride = parameterProperties.getProperty("kinesis.region");
        if (kinesisRegionOverride != null) {
            kinesisRegion = kinesisRegionOverride;
        }

        LOG.info("KinesisSourceToStream (consumer) region "+ kinesisRegion);

        String partitionKeyOverride = parameterProperties.getProperty("kinesis.partitionkey");
        if (partitionKeyOverride != null) {
            partitionKey = partitionKeyOverride;
        }

        LOG.info("KinesisSourceToStream (consumer) partitionkey " + partitionKey);

        String maxRecordsToProcessOverride = parameterProperties.getProperty("kinesis.maxrecordstoprocess");
        if (maxRecordsToProcessOverride != null) {
            maxRecordsToProcess = Integer.parseInt(maxRecordsToProcessOverride);

        }
        LOG.info("KinesisSourceToStream (consumer) maxRecordsToProcess " + maxRecordsToProcess);

        try {
            configure();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static void configure() throws IOException {


        // ensure the JVM will refresh the cached IP values of AWS resources (e.g. service endpoints).
        java.security.Security.setProperty("networkaddress.cache.ttl", "60");

        String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        LOG.info("KinesisSourceToStream (consumer) using workerId: " + workerId);

        // Get credentials from IMDS. If unsuccessful, get them from the credential profiles file.
        AWSCredentialsProvider credentialsProvider = null;
        try {
            LOG.info("KinesisSourceToStream (consumer) trying default aws properties for credentials");
            credentialsProvider = new ProfileCredentialsProvider();
            // Verify we can fetch credentials from the provider
            AWSCredentials credentials = credentialsProvider.getCredentials();
            LOG.info("KinesisSourceToStream (consumer) obtained credentials from aws defaults.");
        } catch (Exception e) {
            LOG.info("KinesisSourceToStream (consumer) Unable to obtain credentials default aws properties", e);
            credentialsProvider = null;
        }

        if (credentialsProvider == null) {
            try {

                credentialsProvider = new InstanceProfileCredentialsProvider(refresh);
                // Verify we can fetch credentials from the provider
                credentialsProvider.getCredentials();
                LOG.info("KinesisSourceToStream (consumer) Obtained credentials from the IMDS.");
            } catch (AmazonClientException e) {
                LOG.info("KinesisSourceToStream (consumer) Unable to obtain credentials from the IMDS", e);
            }
        }

        LOG.info("KinesisSourceToStream (consumer) Using credentials with access key id: " + credentialsProvider.getCredentials().getAWSAccessKeyId());

        kinesisClientLibConfiguration = new KinesisClientLibConfiguration(applicationName, streamName,
                credentialsProvider, workerId).withInitialPositionInStream(initPosInStream); 
    }


    public void processData() {

        IRecordProcessorFactory recordProcessorFactory = new KCRecordProcessorFactory();
        Worker worker = new Worker(recordProcessorFactory, kinesisClientLibConfiguration);
        worker.run();
        worker.shutdown();

    }

    private class KCRecordProcessor implements IRecordProcessor  {

        private Log LOG = LogFactory.getLog(KCRecordProcessor.class);
        private String kinesisShardId;

        // Backoff and retry settings
        private static final long BACKOFF_TIME_IN_MILLIS = 3000L;
        private static final int NUM_RETRIES = 10;

        // Checkpoint about once a minute
        private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L;
        private long nextCheckpointTimeInMillis;

        private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();


        private KCRecordProcessor() {
            super();
        }

        public void initialize(String shardId) {
            LOG.info("KinesisSourceToStream (consumer) Initializing record processor for shard: " + shardId);
            this.kinesisShardId = shardId;
        }

        public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
            LOG.info("KinesisSourceToStream (consumer) Processing " + records.size() + " records from " + kinesisShardId);

            // Process records and perform all exception handling.
            try {
                processRecordsWithRetries(records);
            } catch (IOException e) {
                e.printStackTrace();
            }

            // Checkpoint once every checkpoint interval.
            if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
                try {
                    checkpointer.checkpoint();
                } catch (InvalidStateException e) {
                    e.printStackTrace();
                } catch (ShutdownException e) {
                    e.printStackTrace();
                }
                nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
            }


        }


        private void processRecordsWithRetries(List<Record> records) throws IOException {
            for (Record record : records) {
                boolean processedSuccessfully = false;
                String data = null;
                for (int i = 0; i < NUM_RETRIES; i++) {
                    try {
                        // For this app, we interpret the payload as UTF-8 chars.
                        data = decoder.decode(record.getData()).toString();
                        LOG.info("KinesisSourceToStream (consumer) got record from KStream: " + record.getSequenceNumber() + ", " + record.getPartitionKey() + ", " + data);

                        putDataOnOutputStream(data);

                        processedSuccessfully = true;
                        break;
                    } catch (CharacterCodingException e) {
                        LOG.error("KinesisSourceToStream (consumer) Malformed data: " + data, e);
                        break;
                    } catch (Throwable t) {
                        LOG.warn("KinesisSourceToStream (consumer) Caught throwable while processing record " + record, t);
                    }

                    // backoff if we encounter an exception.
                    try {
                        Thread.sleep(BACKOFF_TIME_IN_MILLIS);
                    } catch (InterruptedException e) {
                        LOG.debug("KinesisSourceToStream (consumer) Interrupted sleep", e);
                    }
                }

                if (!processedSuccessfully) {
                    LOG.error("KinesisSourceToStream (consumer) Couldn't process record " + record + ". Skipping the record.");
                }
                if (stopProcessing) {
                    LOG.info("KinesisSourceToStream (consumer) hit max number of records to process - gracefully shutting down");

                    try {
                        LOG.info("KinesisSourceToStream (consumer) closing output stream");
                        outputStream.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    break;
                }
            }
        }

        public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
            LOG.info("KinesisSourceToStream (consumer) Shutting down record processor for shard: " + kinesisShardId);
            // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
            if (reason == ShutdownReason.TERMINATE) {
                checkpoint(checkpointer);
            }
        }

        public void checkpoint(IRecordProcessorCheckpointer checkpointer) {
            LOG.info("KinesisSourceToStream (consumer) Checkpointing shard " + kinesisShardId);
            for (int i = 0; i < NUM_RETRIES; i++) {
                try {
                    checkpointer.checkpoint();
                    break;
                } catch (ShutdownException se) {
                    // Ignore checkpoint if the processor instance has been shutdown (fail over).
                    LOG.info("KinesisSourceToStream (consumer) Caught shutdown exception, skipping checkpoint.", se);
                    break;
                } catch (ThrottlingException e) {
                    // Backoff and re-attempt checkpoint upon transient failures
                    if (i >= (NUM_RETRIES - 1)) {
                        LOG.error("KinesisSourceToStream (consumer) Checkpoint failed after " + (i + 1) + "attempts.", e);
                        break;
                    } else {
                        LOG.info("KinesisSourceToStream (consumer) Transient issue when checkpointing - attempt " + (i + 1) + " of "
                                + NUM_RETRIES, e);
                    }
                } catch (InvalidStateException e) {
                    // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
                    LOG.error("KinesisSourceToStream (consumer) Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
                    break;
                }
                try {
                    Thread.sleep(BACKOFF_TIME_IN_MILLIS);
                } catch (InterruptedException e) {
                    LOG.debug("KinesisSourceToStream (consumer) Interrupted sleep", e);
                }
            }
            LOG.debug("KinesisSourceToStream (consumer) finished processing");
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
    private void putDataOnOutputStream (String data) throws RuntimeException {

        try {
            outputStream.write(data.getBytes());

            LOG.debug("KinesisSourceToStream (consumer) writing record to piped output stream---> " + recordStringBuffer);
            recordCount++;

            if (recordCount >= maxRecordsToProcess & maxRecordsToProcess > -1) {
                LOG.info("KinesisSourceToStream (consumer) max records to process limit achieved");
                stopProcessing = true;
            }

            recordStringBuffer.setLength(0);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}