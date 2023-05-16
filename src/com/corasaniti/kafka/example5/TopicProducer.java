package com.corasaniti.kafka.example5;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.corasaniti.kafka.common.message.MessageTime;
import com.corasaniti.kafka.common.util.ReadProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Properties;

public class TopicProducer {
    private static final Logger log = LoggerFactory.getLogger(TopicProducer.class);
    private static final String TOPIC_NAME = "demojava2";

    public static void main(String[] args) throws Exception  {
        log.info("I am a Kafka Producer");
        
        //Get KafkaServer
        String bootstrapServers = ReadProperties.getProperty(ReadProperties.KAFKA_SERVERS);

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //Create a Producer record JSON format
        MessageTime messageTime = new MessageTime("Hello from Kafka");
        ObjectMapper objectMapper = new ObjectMapper();
        String messageJSON = objectMapper.writeValueAsString(messageTime);
        
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_NAME, messageJSON);

        // send data - asynchronous
        producer.send(producerRecord);

        // flush data - synchronous
        producer.flush();
        // flush and close producer
        producer.close();
    }
}