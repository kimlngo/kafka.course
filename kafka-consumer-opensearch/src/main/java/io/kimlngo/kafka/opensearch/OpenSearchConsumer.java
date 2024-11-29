package io.kimlngo.kafka.opensearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {
    private static final Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());
    private static final String INDEXES = "wikimedia";
    private static final String GROUP_ID = "consumer-opensearch-demo";

    public static void main(String[] args) throws IOException {
        //create OpenSearch client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        //create Kafka Consumer
        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer();

        //get a reference to main thread
        final Thread mainThread = Thread.currentThread();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected a shutdown, let's exit by calling consumer.wakeup() ...");
            kafkaConsumer.wakeup();

            //join the main thread to allow execution of the code in the main thread
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try (openSearchClient; kafkaConsumer) {
            boolean isExist = openSearchClient.indices()
                                              .exists(new GetIndexRequest(INDEXES), RequestOptions.DEFAULT);

            if (!isExist) {
                CreateIndexRequest request = new CreateIndexRequest(INDEXES);
                openSearchClient.indices()
                                .create(request, RequestOptions.DEFAULT);

                log.info("The wikimedia index has been created");
            } else {
                log.info("The wikimedia is already existed");
            }

            kafkaConsumer.subscribe(Collections.singleton("wikimedia.recentchange"));

            while(true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(3000));

                int recordCount = records.count();
                log.info("Received " + recordCount + " record(s)");

                BulkRequest bulkRequest = new BulkRequest();

                for(var record : records) {
                    //strategy 1: self-define an ID for idempotency
//                    String id = String.format("%s_%d_%d", record.topic(), record.partition(), record.offset());

                    try {
                        //strategy 2: extract id from record.value()/meta/id.
                        String id = extractId(record.value());
                        IndexRequest indexRequest = new IndexRequest(INDEXES).source(record.value(), XContentType.JSON)
                                                                             .id(id);

                        bulkRequest.add(indexRequest);
//                        IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);

                    } catch (Exception e) {
                    }
                }

                if(bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Inserted " + bulkResponse.getItems().length + " record(s)");

                    Thread.sleep(1000);

                    kafkaConsumer.commitSync();
                    log.info("Offset committed");
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer is starting to shutdown");
        } catch (Exception e) {
            log.error("Unexpected exception in the consumer", e);
        } finally {
            kafkaConsumer.close();
            openSearchClient.close();
            log.info("The consumer is now gracefully shutdown ...");
        }
    }

    private static String extractId(String json) {
        //using gson lib
        return JsonParser.parseString(json)
                         .getAsJsonObject()
                         .get("meta")
                         .getAsJsonObject()
                         .get("id")
                         .getAsString();
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {


        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");

        //config serializer
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        //create consumer
        return new KafkaConsumer<>(properties);
    }

    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                              .setHttpClientConfigCallback(
                                      httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                                                                      .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }
}
