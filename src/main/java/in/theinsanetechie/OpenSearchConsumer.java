package in.theinsanetechie;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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

    /**
     * Create an Open Search Client
     *
     * @return the created client
     */
    private static RestHighLevelClient createOpenSearchClient() {
        RestHighLevelClient restHighLevelClient;

        String connString = "http://localhost:9200";
        // we build a URI from the connection string
        URI connUri = URI.create(connString);

        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) { // REST client without security
            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));
        } else { // REST client with security
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

    /**
     * Create a Kafka Consumer
     *
     * @return the created consumer
     */
    private static KafkaConsumer<String, String> createKafkaConsumer() {
        String boostrapServers = "127.0.0.1:9092";
        String groupId = "opensearch-consumer-demo";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // create consumer
        return new KafkaConsumer<>(properties);
    }

    public static void main(String[] args) throws IOException {
        // logger
        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

        // create an open search client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        // create a Kafka consumer
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        String indexName = "wikimedia";
        String topic = "wikimedia.recentChanges";

        try (openSearchClient; consumer) {
            // check whether index already exists
            GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
            boolean indexExists = openSearchClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);

            if (!indexExists) { // create index if not already exists
                CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("Index: \"" + indexName + "\" created!");
            } else {
                log.info("Index: \"" + indexName + "\" already exists!");
            }

            // subscribe to the topic
            consumer.subscribe(Collections.singleton(topic));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
                int recordCount = records.count();
                log.info("Received " + recordCount + " record(s)");

                // create bulk request
                BulkRequest bulkRequest = new BulkRequest();

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        // create request to send record to Open Search
                        IndexRequest indexRequest = new IndexRequest(indexName).source(record.value(), XContentType.JSON);
                        // add to bulk request
                        bulkRequest.add(indexRequest);
                    } catch (Exception exp) {
                        log.error("Exception occurred: ", exp);
                    }
                }

                // insert bulk request
                if (bulkRequest.numberOfActions() > 0){
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Inserted " + bulkResponse.getItems().length + " record(s).");

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    // commit offsets after the batch is consumed
                    consumer.commitSync();
                    log.info("Offsets have been committed!");
                }
            }
        }

    }
}
