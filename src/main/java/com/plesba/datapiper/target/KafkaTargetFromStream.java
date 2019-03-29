package com.plesba.datapiper.target;

import com.amazonaws.AmazonServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.PipedInputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.net.InetAddress;

//read input stream; write to kafka stream (producer)
public class KafkaTargetFromStream {

    private PipedInputStream inputStream;
    private int recordCount =0;
    private String streamStatus= null;

    private static final String DEFAULT_ACKS = "1";
    private static final String DEFAULT_CLIENTID = "localhost";
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_TOPIC_NAME = "rsdKFStream1";
    private static final String DEFAULT_KEY_SERIALIZER = "defaultkey";
    private static final String DEFAULT_VALUE_SERIALIZER = "defaultvalue";
    private static final int DEFAULT_MAX_RECORDS_TO_PROCESS = -1;

    private static String acks = DEFAULT_ACKS;
    private static String clientId = DEFAULT_CLIENTID;
    private static String bootstrapServers = DEFAULT_BOOTSTRAP_SERVERS;
    private static String topicName = DEFAULT_TOPIC_NAME;
    private static Integer maxRecordsToProcess = DEFAULT_MAX_RECORDS_TO_PROCESS;

    private static String keySerializer = DEFAULT_KEY_SERIALIZER;
    private static String valueSerializer = DEFAULT_VALUE_SERIALIZER;

    private boolean stopProcessing=false;
    private Producer<String, String> producer;
    private static final Log LOG = LogFactory.getLog(KafkaTargetFromStream.class);

    public KafkaTargetFromStream(Properties parameterProperties, PipedInputStream parameterInputStream) throws InterruptedException {

        inputStream = parameterInputStream;

        String acksOverride = parameterProperties.getProperty("kafka.acks");
        if (acksOverride != null) {
            acks = acksOverride;
        }

        LOG.info("KafkaTargetFromStream using acks " + acks);

        String clientIdOverride = parameterProperties.getProperty("kafka.client.id");
        if (clientIdOverride != null) {
            clientId = clientIdOverride;
        } else {
            try {
            clientId = InetAddress.getLocalHost().getHostName();
            } catch (Exception ex) {
                Logger.getLogger(KafkaTargetFromStream.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        LOG.info("KafkaTargetFromStream using clientId " + clientId);

        String bootstrapserversOverride = parameterProperties.getProperty("kafka.bootstrap.servers");
        if (bootstrapserversOverride != null) {
            bootstrapServers = bootstrapserversOverride;
        }

        LOG.info("KafkaTargetFromStream using clientId " + clientId);


        String topicNameOverride = parameterProperties.getProperty("kafka.topic.name");
        if (topicNameOverride != null) {
            topicName = topicNameOverride;
        }

        LOG.info("KafkaTargetFromStream using topicName " + topicName);

        String maxRecordsToProcessOverride = parameterProperties.getProperty("kafka.maxrecordstoprocess");
        if (maxRecordsToProcessOverride != null) {
            maxRecordsToProcess = Integer.parseInt(maxRecordsToProcessOverride);

        }
        LOG.info("KafkaTargetFromStream using maxrecordstoprocess " + maxRecordsToProcess);

        String keySerializerOverride = parameterProperties.getProperty("key.serializer");
        if (keySerializerOverride != null) {
            keySerializer = keySerializerOverride;

        }

        LOG.info("KafkaTargetFromStream using keySerializer " + maxRecordsToProcess);

        String valueSerializerOverride = parameterProperties.getProperty("value.serializer");
        if (valueSerializerOverride != null) {
            valueSerializer = valueSerializerOverride;

        }

        LOG.info("KafkaTargetFromStream using value serializer " + valueSerializer);

        Producer<String, String> producer = new KafkaProducer
                <String, String>(parameterProperties);

        LOG.info("KafkaTargetFromStream created Kafka Producer ");

    }

    public int GetLoadedCount() {
        return this.recordCount;
    }

    public void processDataFromInputStream() {

        LOG.info("KafkaTargetFromStream started stream processing " + topicName);

        StringBuilder recordStringBuffer = new StringBuilder();
        String streamRecord = new String();

        try {

            int streamByte = inputStream.read();

            while (streamByte != -1 & !stopProcessing) {   //end of stream
                if (streamByte != 10) {  //end of line
                    recordStringBuffer.append((char) streamByte);
                } else { // process record
                    streamRecord = recordStringBuffer.toString() + '\n';
                    ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topicName, streamRecord);
                    LOG.info("KafkaTargetFromStream record to put placed on stream: " + streamRecord);
                    try {
                        producer.send(rec);
                    } catch (Exception ex) {
                        Logger.getLogger(KafkaTargetFromStream.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    recordCount = recordCount + 1;
                    recordStringBuffer.setLength(0);
                    if (recordCount >= maxRecordsToProcess & maxRecordsToProcess > -1 ){ stopProcessing = true;}

                    LOG.info("KafkaTargetFromStream records written to stream: " + recordCount);
                }


                streamByte = inputStream.read();
            }

            producer.close();

            LOG.info("KafkaTargetFromStream completed and processed "+ recordCount + " records.");

        } catch(Exception ex){

            LOG.error("KafkaTargetFromStream error detected in processDataFromInputStream " ,ex);

        }

        LOG.info("KafkaTargetFromStream (Producer) finished processing");
    }


}
