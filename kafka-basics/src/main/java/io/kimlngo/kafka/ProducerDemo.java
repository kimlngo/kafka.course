package io.kimlngo.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    //constant
    private static final String TOPIC_NAME = "demo-java";

    public static void main(String[] args) {
        log.info("Hello World");

        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //config serializer
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //create Producer Record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_NAME, "Today is Friday");

        //send data
        producer.send(producerRecord);

        //flush and close the producer
        //tell producer to send all data and block until done - synchronous
        producer.flush();
        //close the producer
        producer.close();
    }
}
