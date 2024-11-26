package io.kimlngo.kafka.consumer;

import io.kimlngo.kafka.config.Config;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemoCooperative {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Consumer Demo");
        String groupId = "my-java-application";

        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //config serializer
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest"); //read --from-beginning | "latest" - read from latest
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //get a reference to main thread
        final Thread mainThread = Thread.currentThread();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup() ...");
                consumer.wakeup();

                //join the main thread to allow execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            //subscribe to a topic
            consumer.subscribe(List.of(Config.TOPIC_NAME));

            //polling for data
            while (true) {
                //poll, if no data, willing to wait 1000ms
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info(String.format("Key: %s, Value: %s", record.key(), record.value()));
                    log.info(String.format("Partition: %d, Offset: %d", record.partition(), record.offset()));
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer is starting to shutdown");
        } catch (Exception e) {
            log.error("Unexpected exception in the consumer", e);
        } finally {
            log.info("The consumer is now gracefully shutdown ...");
        }
    }
}
