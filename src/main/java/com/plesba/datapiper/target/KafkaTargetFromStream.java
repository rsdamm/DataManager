package com.plesba.datapiper.target;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.kafka.clients.producer.*;

import java.io.PipedInputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.net.InetAddress;

//read input stream; write to kafka stream (producer)
public class KafkaTargetFromStream {

    private PipedInputStream inputStream;
    private int recordCount =0;

    private static final String DEFAULT_ACKS = "1";
    private static final String DEFAULT_CLIENTID = "localhost";
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_TOPIC = "test";
    private static final String DEFAULT_KEY_SERIALIZER = "defaultkey";
    private static final String DEFAULT_VALUE_SERIALIZER = "defaultvalue";
    private static final String DEFAULT_PRODUCER_TYPE = "sync";
    private static final int DEFAULT_MAX_RECORDS_TO_PROCESS = -1;

    private static String acks = DEFAULT_ACKS;
    private static String clientId = DEFAULT_CLIENTID;
    private static String bootstrapServers = DEFAULT_BOOTSTRAP_SERVERS;
    private static String topic = DEFAULT_TOPIC;
    private static Integer maxRecordsToProcess = DEFAULT_MAX_RECORDS_TO_PROCESS;
    private static String keySerializer = DEFAULT_KEY_SERIALIZER;
    private static String valueSerializer = DEFAULT_VALUE_SERIALIZER;
    private static String producerType = DEFAULT_PRODUCER_TYPE;

    private boolean stopProcessing=false;
    private Producer<String, String> producer;
    private static final Log LOG = LogFactory.getLog(KafkaTargetFromStream.class);

    public KafkaTargetFromStream(Properties parameterProperties, PipedInputStream parameterInputStream) throws InterruptedException {

        LOG.info("KafkaTargetFromStream (producer) started processing.");

        inputStream = parameterInputStream;

        String acksOverride = parameterProperties.getProperty("acks");
        if (acksOverride != null) {
            acks = acksOverride;
        }

        LOG.info("KafkaTargetFromStream using acks " + acks);

        String clientIdOverride = parameterProperties.getProperty("client.id");
        if (clientIdOverride != null) {
            clientId = clientIdOverride;
        } else {
            try { clientId = InetAddress.getLocalHost().getHostName();
            } catch (Exception ex) {
                Logger.getLogger(KafkaTargetFromStream.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        LOG.info("KafkaTargetFromStream using clientId " + clientId);

        String bootstrapserversOverride = parameterProperties.getProperty("bootstrap.servers");
        if (bootstrapserversOverride != null) {
            bootstrapServers = bootstrapserversOverride;
        }

        LOG.info("KafkaTargetFromStream using bootstrapservers " + bootstrapServers);


        String topicOverride = parameterProperties.getProperty("topic");
        if (topicOverride != null) {
            topic = topicOverride;
        }

        LOG.info("KafkaTargetFromStream using topic " + topic);

        String maxRecordsToProcessOverride = parameterProperties.getProperty("maxrecordstoprocess");
        if (maxRecordsToProcessOverride != null) {
            maxRecordsToProcess = Integer.parseInt(maxRecordsToProcessOverride);

        }
        LOG.info("KafkaTargetFromStream using maxrecordstoprocess " + maxRecordsToProcess);

        String keySerializerOverride = parameterProperties.getProperty("key.serializer");
        if (keySerializerOverride != null) {
            keySerializer = keySerializerOverride;

        }

        LOG.info("KafkaTargetFromStream using key serializer " + keySerializer);

        String valueSerializerOverride = parameterProperties.getProperty("value.serializer");
        if (valueSerializerOverride != null) {
            valueSerializer = valueSerializerOverride;

        }

        LOG.info("KafkaTargetFromStream using value serializer " + valueSerializer);

        String producerTypeOverride = parameterProperties.getProperty("producer.type");
        if (producerTypeOverride != null) {
            producerType = producerTypeOverride;

        }

        LOG.info("KafkaTargetFromStream using producer type (sync vs async) " + producerType);


        producer = new KafkaProducer <String, String>(parameterProperties);

        LOG.info("KafkaTargetFromStream created Kafka Producer ");

    }

    public int GetLoadedCount() {
        return this.recordCount;
    }

    public void processDataFromInputStream() {

        LOG.info("KafkaTargetFromStream started stream processing " + topic);
        LOG.info("KafkaTargetFromStream mode: " + producerType  );

        StringBuilder recordStringBuffer = new StringBuilder();
        String streamRecord = new String();

        try {

            int streamByte = inputStream.read();

            while (streamByte != -1 & !stopProcessing) {   //end of stream
                if (streamByte != 10) {  //end of line
                    recordStringBuffer.append((char) streamByte);
                } else { // write record to stream
                    streamRecord = recordStringBuffer.toString() + '\n';
                    ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topic, streamRecord);
                    LOG.info("KafkaTargetFromStream record to put placed on stream: " + streamRecord);
                    try {
                        if (producerType.equals ("sync")) { //sync
                            producer.send(rec);
                        }
                        else producer.send(rec, new producerCallback()); //async

                    } catch (Exception ex) {
                        Logger.getLogger(KafkaTargetFromStream.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    recordCount = recordCount + 1;
                    recordStringBuffer.setLength(0);
                    if (recordCount >= maxRecordsToProcess & maxRecordsToProcess > -1 ){ stopProcessing = true;}

                    LOG.info("KafkaTargetFromStream records written to stream " + producerType + " : " + recordCount);
                }

                streamByte = inputStream.read();
            }

            LOG.info("KafkaTargetFromStream completed and processed "+ recordCount + " records.");

        } catch(Exception ex){

            LOG.error("KafkaTargetFromStream error detected in processDataFromInputStream " ,ex);

        } finally {
            producer.close();
        }

        LOG.info("KafkaTargetFromStream (Producer) finished processing");
    }
    class producerCallback implements Callback {

        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null)
                System.out.println("KafkaTargetFromStream async failed with an exception");
            else
                System.out.println("KafkaTargetFromStream async call successful");
        }
    }
}
