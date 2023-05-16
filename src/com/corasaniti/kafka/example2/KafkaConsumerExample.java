package com.corasaniti.kafka.example2;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaConsumerExample {
    public static void main(String[] args) {
    	
    	//specifica il nome della tua coda Kafka
    	String topicName = "demojava";
    	//specifica il nome del tuo gruppo di consumatori
        String groupName = "demojava-group"; 

        // Configura le proprietà del consumatore
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.2.34:9092");
        props.put("group.id", groupName);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // Crea un consumatore Kafka
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Iscriviti alla coda Kafka
        consumer.subscribe(Arrays.asList(topicName));

        // Leggi i messaggi dalla coda Kafka
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("Messaggio letto: offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
    }
}
