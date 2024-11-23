package io.kimlngo.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.IntStream;

public class ProducerDemoWithKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class.getSimpleName());

    //constant
    private static final String TOPIC_NAME = "demo-java-3p";

    public static void main(String[] args) {
        log.info("Producer Demo With Keys");

        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //config serializer
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("batch.size", "400");

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        IntStream.range(0, 2)
            .forEach(j -> {
                 IntStream.range(0, 30)
                          .forEach(i -> {
                              String key = "id_" + i;
                              String value = "Hi " + i;
                              //create Producer Record
                              ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_NAME, key, value);

                              //send data
                              producer.send(producerRecord, new Callback() {
                                  @Override
                                  public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                                      if (e == null) {
                                          //success case
                                          log.info(String.format("Key: %s | Partition: %d", key, recordMetadata.partition()));
                                      } else {
                                          log.error("Error while sending data", e);
                                      }
                                  }
                              });
                          });
                 log.info("Finished batch " + (j + 1) + ", sleep for 500ms now ...");
                 try {
                     Thread.sleep(1000);
                 } catch (InterruptedException e) {
                     throw new RuntimeException(e);
                 }
            });
        //flush and close the producer
        //tell producer to send all data and block until done - synchronous
        producer.flush();
        //close the producer
        producer.close();
    }
}
