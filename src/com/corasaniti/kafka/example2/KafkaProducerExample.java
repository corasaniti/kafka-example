package com.corasaniti.kafka.example2;

import java.util.Properties;
import org.apache.kafka.clients.producer.*;

public class KafkaProducerExample {
    public static void main(String[] args) {
    	
    	// specifica il nome della tua coda Kafka
        String topicName = "demojava"; 
        
        // specifica il messaggio da scrivere
        String message = "Hello, Kafka..."; 

        // Configura le proprietà del produttore
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.2.34:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Crea un produttore Kafka
        Producer<String, String> producer = new KafkaProducer<>(props);

        // Crea un record da inviare alla coda Kafka
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, message);

        // Invia il record alla coda Kafka
        producer.send(record);

        // Chiudi il produttore Kafka
        producer.close();
    }
}
