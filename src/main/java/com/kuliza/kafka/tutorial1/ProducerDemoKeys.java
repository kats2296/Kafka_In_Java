package com.kuliza.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        String bootstrapServer = "127.0.0.1:9092";
        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        // Step 1) creating producer properties
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Step 2) creating the producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(producerProperties);

        // Step 3) sending the data
        // creating a producer record
        for(int i=0;i<10;i++) {
            String topic = "first_topic";
            String value = "hello world !!" + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            logger.error("Key : " + key);

            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes everytime a record is sent successfully or if an exception is thrown
                    if (e == null) {
                        logger.info("Received new metadata " + "\n" +
                                "Topic : " + recordMetadata.topic() + "\n" +
                                "Partition : " + recordMetadata.partition() + "\n" +
                                "Offset : " + recordMetadata.offset() + "\n" +
                                "Timestamp" + recordMetadata.timestamp() + "\n");
                    } else {
                        logger.error("Error while producing : " + e);
                    }
                }
            }).get(); //forcing the send to be synchronous; don't do it in general
        }

        producer.flush();
        producer.close();
    }
}
