package com.corasaniti.kafka.example3;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.corasaniti.kafka.common.message.MessageTime;
import com.fasterxml.jackson.databind.ObjectMapper;


public class KafkaJsonConsumerExample {
    public static void main(String[] args) 
    {
    	//specifica il nome della tua coda Kafka
    	String topicName = "demojava-json"; 

        // Configura le proprietà del consumatore
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.2.34:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Crea un consumatore Kafka
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Iscriviti alla coda Kafka
        consumer.subscribe(Collections.singletonList(topicName));

        // Leggi i messaggi dalla coda Kafka
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                try {
                    // Deserializza il messaggio JSON in un oggetto personalizzato
                    ObjectMapper objectMapper = new ObjectMapper();
                    MessageTime myData = objectMapper.readValue(record.value(), MessageTime.class);

                    // Elabora l'oggetto personalizzato
                    System.out.printf("Received message: id=%s, message=%s, time=%d\n", myData.id, myData.message, myData.timestamp);

                } catch (Exception e) {
                    System.out.println("Error processing message: " + e.getMessage());
                }
            }
        }
    }
}


