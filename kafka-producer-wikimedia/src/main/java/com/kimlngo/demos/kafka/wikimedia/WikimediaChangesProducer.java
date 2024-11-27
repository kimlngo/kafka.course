package com.kimlngo.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {
    private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    private static final String TOPIC = "wikimedia.recentchange";
    private static final String STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange";

    public static void main(String[] args) throws InterruptedException {
        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(createProducerProperties());

        EventHandler eventHandler = new WikimediaChangeHandler(producer, TOPIC);
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(STREAM_URL));
        EventSource eventSource = builder.build();

        //start producer in another thread
        eventSource.start();

        //produce for 10 minutes and block the program till then
        TimeUnit.MINUTES.sleep(10);
    }

    private static Properties createProducerProperties() {
        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", BOOTSTRAP_SERVER);

        //config serializer
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //enable high throughput producer configs
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(32 * 1024));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        return properties;
    }
}
