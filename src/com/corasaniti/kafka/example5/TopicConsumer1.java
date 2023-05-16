package com.corasaniti.kafka.example5;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.corasaniti.kafka.common.message.MessageTime;
import com.corasaniti.kafka.common.util.ReadProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TopicConsumer1 {
    
	private static final String TOPIC_NAME = "demojava2";
    private static final String GROUP_ID = "demojava2-groupa";

    public static void main(String[] args) throws Exception 
    {    	
        //Get KafkaServer
        String bootstrapServers = ReadProperties.getProperty(ReadProperties.KAFKA_SERVERS);
    	
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        @SuppressWarnings("resource")
		KafkaConsumer<String, String> consumer1 = new KafkaConsumer<>(props);
        consumer1.subscribe(Collections.singleton(TOPIC_NAME));

        System.out.printf("Consumer1 waiting for message... ");        
        while (true) {
            ConsumerRecords<String, String> records1 = consumer1.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records1) {
            	try {
	                //Deserializza il messaggio JSON in un oggetto personalizzato
	                ObjectMapper objectMapper = new ObjectMapper();
	                MessageTime messageTime = objectMapper.readValue(record.value(), MessageTime.class);
                    // Elabora l'oggetto personalizzato
                    System.out.printf("Consumer1 Received message: id=%s, message=%s, time=%s, DateTime=%s\n", 
                    		   		   messageTime.id, 
                    		   		   messageTime.message, 
                    		   		   messageTime.timestamp,
                    		   		   messageTime.dateTime
                    		   		  );
            	} 
            	catch (Exception e) {
            		System.out.println("Error processing message: " + e.getMessage());
            	}
            }
        }
    }
}
