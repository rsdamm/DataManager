package com.plesba.datapiper.source;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.PipedOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

//reads kafka stream writes to output stream
public class KafkaSourceToStream {

    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_GROUP_ID_CONFIG = "rsdKFGroup1";
    private static final String DEFAULT_KEY_DESERIALIZER = "defaultkey";
    private static final String DEFAULT_VALUE_DESERIALIZER = "defaultvalue";
    private static final int DEFAULT_MAX_RECORDS_TO_PROCESS = 1;
    private static final String DEFAULT_TOPIC = "test";

    private static String bootstrapServers = DEFAULT_BOOTSTRAP_SERVERS;
    private static String groupIdConfig = DEFAULT_GROUP_ID_CONFIG;
    private static Integer maxRecordsToProcess = DEFAULT_MAX_RECORDS_TO_PROCESS;
    private static String keyDeSerializer = DEFAULT_KEY_DESERIALIZER;
    private static String topic = DEFAULT_TOPIC;
    private static String valueDeserializer = DEFAULT_VALUE_DESERIALIZER;
    private final Consumer<Long, String> consumer;

    private byte[] theByteArray = null;
    private StringBuilder recordStringBuffer;
    private int i=0;
    private String nextLine = null;
    private PipedOutputStream outputStream;
    private int recordCount = 0;
    private boolean stopProcessing=false;

    private static boolean refresh = true;

    private static final Log LOG = LogFactory.getLog(KafkaSourceToStream.class);

    public KafkaSourceToStream(Properties parameterProperties, PipedOutputStream parameterOutputStream) {
        recordStringBuffer = new StringBuilder();

        LOG.info("KafkaSourceToStream (consumer) started processing.");

        outputStream = parameterOutputStream;

        LOG.info(" testing parameter access " + parameterProperties.getProperty("bootstrap.servers"));

        String bootstrapserversOverride = parameterProperties.getProperty("bootstrap.servers");
        if (bootstrapserversOverride != null) {
            bootstrapServers = bootstrapserversOverride;
        }

        LOG.info("KafkaSourceToStream using bootstrapservers " + bootstrapServers);

        String maxRecordsToProcessOverride = parameterProperties.getProperty("maxrecordstoprocess");
        if (maxRecordsToProcessOverride != null) {
            maxRecordsToProcess = Integer.parseInt(maxRecordsToProcessOverride);

        }
        LOG.info("KafkaSourceToStream using maxrecordstoprocess " + maxRecordsToProcess);

        String groupIdConfigOverride = parameterProperties.getProperty("group_id");
        if (groupIdConfigOverride != null) {
            groupIdConfig = groupIdConfigOverride;

        }
        LOG.info("KafkaSourceToStream using groupId " + groupIdConfig);


        String keyDeserializerOverride = parameterProperties.getProperty("key.deserializer");
        if (keyDeSerializer != null) {
            keyDeSerializer = keyDeserializerOverride;

        }

        LOG.info("KafkaSourceToStream using key deserializer " + keyDeSerializer);

        String valueDeserializerOverride = parameterProperties.getProperty("value.deserializer");
        if (valueDeserializerOverride != null) {
            valueDeserializer = valueDeserializerOverride;

        }

        LOG.info("KafkaSourceToStream using value deserializer " + valueDeserializer);

        String topicOverride = parameterProperties.getProperty("topic");
        if (topicOverride != null) {
            topic = topicOverride;
        }

        LOG.info("KafkaSourceToStream using topic " + topic);

        //create the consumer
        consumer = new KafkaConsumer(parameterProperties);
        LOG.info("KafkaSourceToStream Consumer created ");


    }
    public int getReadCount ()    {
        return this.recordCount ;
    }

    public void processData() {

        LOG.info("KafkaSourceToStream (consumer) processData started");

        //subscribe to topic
        consumer.subscribe(Collections.singletonList(topic));


        LOG.info("KafkaSourceToStream (consumer) topic subscribed");

        Duration d = Duration.ofSeconds(10);


        LOG.info("KafkaSourceToStream (consumer) starting loop");

        while (true) {
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(d);


            LOG.info("KafkaSourceToStream (consumer) polled");



            consumerRecords.forEach(record -> {
                putDataOnOutputStream("Key: " + record.key() + " Value: " + record.value() + " Partition: " + record.partition() + " Offset: " +record.offset());
            });

            if (consumerRecords.count()>0) {
                if (recordCount > maxRecordsToProcess & maxRecordsToProcess > -1) break;
                else continue;
            }
            consumer.commitAsync();
        }
        consumer.close();
        try {
            LOG.info("KafkaSourceToStream (consumer) closing output stream");
            outputStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println("KafkaSourceToStream (consumer) - processing completed processing "+ recordCount + " records.");


    }

    private void putDataOnOutputStream(String data) throws RuntimeException {

        try {
            outputStream.write(data.getBytes());
            LOG.debug("KafkaSourceToStream (consumer) writing record to piped output stream-----> " + data);
            recordCount++;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}