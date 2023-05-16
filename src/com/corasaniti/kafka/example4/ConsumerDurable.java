package com.corasaniti.kafka.example4;

import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;


/*
 * Ecco un esempio di una classe consumer che mostra la differenza
 * tra una sottoscrizione-duratura (persistent subscription) e una
 * ed una sottosccrizione non duratura (non-persistent subscription)
 * 
 * Vengono creati due consumer iscritti allo stesso topic creato in 
 * modo persistente e si evidenzia come quello sottoscritto in 
 * modalità duratura ricevi anche i messaggi inviati al topic prima 
 * della sua sottoscrizione, mentre, quello con  sottoscrizione non 
 * duratura riceve i messaggi inviati al topic solo quando è 
 * attivaemnte connesso al sistema di messaggistica.
 * Questi non riceverà i messaggi inviati al topic prima della sua
 * sottoscrizione.
 * 
 * Il topic demojava-persistent dev essere creato con sottoscrizione
 * persistente attraverso il seguente comando 
 * kafka-topics.sh --create --topic emojava-persisten --partitions 3 --config cleanup.policy=compact --config retention.ms=-1
 * 
 */

public class ConsumerDurable {
    private static final String TOPIC_NAME = "demojava2";
    private static final String GROUP_ID   = "demojava2-group-b";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.2.34:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Sottoscrizione duratura
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 10000);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        KafkaConsumer<String, String> durableConsumer = new KafkaConsumer<>(props);
        durableConsumer.subscribe(Arrays.asList(TOPIC_NAME));


        try {
            while (true)  {
            	/* Sottoscrizione duratura
            	 * Nella sottoscrizione duratura, un consumer riceve tutti i messaggi 
            	 * inviati al topic anche se si è iscritto  al  topic successivamente 
            	 * all'invio del messaggio. I messaggi  vengono infatti mantenuti nel 
            	 * topic fino a quando non vengono esplicitamente rimossi eeliminati 
            	 * dal consumer o dal sistema di messaggistica.
            	 */            	
                var records = durableConsumer.poll(java.time.Duration.ofSeconds(1));
                for (var record : records) {
                    System.out.println("Durable Consumer: Received message: " + record.value());
                }
                durableConsumer.commitSync();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            durableConsumer.close();
        }
    }
}
