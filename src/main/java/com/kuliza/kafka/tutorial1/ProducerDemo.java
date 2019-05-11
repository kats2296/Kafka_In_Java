package com.kuliza.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {

        String bootstrapServer = "127.0.0.1:9092";

        // Step 1) creating producer properties
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Step 2) creating the producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(producerProperties);

        // Step 3) sending the data
        // creating a producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world!!");
        producer.send(record); //this is asynchronous so flush/close the producer
        producer.flush();
        producer.close();
    }
}
