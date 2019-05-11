package com.kuliza.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-third-application";
        String topic = "first_topic";

        // Step 1) Create consumer properties
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Step 2) Creating the consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(consumerProperties);

        // Step 3) Subscribing to topic(s)
        //consumer.subscribe(Collections.singleton(topic)); //when you want to subscribe to just one topic
        consumer.subscribe(Arrays.asList(topic)); //when you want to subscribe to multiple topics add them to a list

        // Step 4) Poll for new data
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String,String> record : records) {
                logger.info("Key : "+ record.key() + "Value : " + record.value());
                logger.info("Partition : " + record.partition() + "Offset : " + record.offset());
            }
        }

    }

}
