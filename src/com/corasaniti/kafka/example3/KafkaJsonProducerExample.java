package com.corasaniti.kafka.example3;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.corasaniti.kafka.common.message.MessageTime;
import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaJsonProducerExample {
    public static void main(String[] args) throws Exception 
    {    
    	String topicName = "demojava-json"; // specifica il nome della tua coda Kafka
        
        //crea un oggetto personalizzato da inviare come messaggio
    	MessageTime myData = new MessageTime("Hello, Kafka!"); 

        // Configura le proprietà del produttore
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.2.34:9092");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Crea un produttore Kafka
        Producer<String, String> producer = new KafkaProducer<>(properties);

        // Serializza l'oggetto personalizzato in formato JSON
        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(myData);

        // Crea un record da inviare alla coda Kafka con il messaggio JSON
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, json);

        // Invia il record alla coda Kafka
        producer.send(record);

        // Chiudi il produttore Kafka
        producer.close();
    }
}

