package com.sufiyan.opensearch;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

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

import com.google.gson.JsonParser;

public class OpenSearchConsumer {
	/*
	 * Here we are going to create below things :
	 * 
	 * 1 : first create an open search client.
	 * 
	 * 2 : create our kafka client.
	 * 
	 * 3 : main code logic.
	 * 
	 * 4 : close things.
	 */

	// Step 1 :
	@SuppressWarnings("resource")
	public static RestHighLevelClient createOpenSearchClient() {
		String connString = "http://localhost:9200"; // docker local host url.
		// String connString = ""; // you can also use bonsai cosol url.

		// We build a uri from connection string.
		RestHighLevelClient restHighLevelClient;
		URI connUri = URI.create(connString);

		// extract login information if exists.
		String userInfo = connUri.getUserInfo();

		if (userInfo == null) {
			// Rest client without security.
			restHighLevelClient = new RestHighLevelClient(
					RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));
		} else {
			// Rest client with security.
			String[] auth = userInfo.split(":");
			CredentialsProvider cp = new BasicCredentialsProvider();
			cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

			restHighLevelClient = new RestHighLevelClient(
					RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
							.setHttpClientConfigCallback(
									HttpAsyncClientBuilder -> HttpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
											.setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));
		}
		return restHighLevelClient;
	}

	private static KafkaConsumer<String, String> createKafkaConsumer() {
		String bootsrapservers = "127.0.0.1:9092";
		String groupId = "consumer-opensearch-demo";

		// Create Consumer properties
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootsrapservers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

		return new KafkaConsumer<String, String>(properties);
	}

	private static String extractId(String json) {
		// gson library;
		return JsonParser.parseString(json).getAsJsonObject().get("meta").getAsJsonObject().get("id").getAsString();
	}

	public static void main(String[] args) throws IOException {
		Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

		// Step 1 :
		RestHighLevelClient openSearchClient = createOpenSearchClient();

		// Step 2 : create our kafka client.
		KafkaConsumer<String, String> consumer = createKafkaConsumer();

		// We need to create index on openSeach if it's doesn't exist.
		try (openSearchClient; consumer) {
			/*
			 * But before creating a new index first check whether it is already exist or
			 * not.
			 */

			boolean indexExist = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"),
					RequestOptions.DEFAULT);

			if (!indexExist) {
				CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
				openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
				log.info("The wikimedia index has been created😊!!!");
			} else {
				log.info("The Wikimedia index already exist😊");
			}

			// we subscribe consumer.
			consumer.subscribe(Collections.singleton("wikimedia.recentchange"));

			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
				int recordCount = records.count();
				log.info("Recieved: " + recordCount + "record(s)");

				for (ConsumerRecord<String, String> record : records) {
					/*
					 * Now let's make consumer idempotence. There are two way to do that.
					 * 
					 * 1 : Define an id using kafka Record coordinations & pass to indexRequest.
					 * 
					 * 2 : Extract value from Json meta which all messages have & pass to
					 * indexRequest.
					 */

					// Strategy 1 :
					String id = record.topic() + "_" + record.partition() + "_" + record.offset();
					log.info("id coming from coordination: " + id);

					// send the record to open search.
					try {

						// Strategy 2 :
						String id1 = extractId(record.value());
						IndexRequest indexRequest = new IndexRequest("wikimedia")
								.source(record.value(), XContentType.JSON).id(id1);
						IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
						log.info(response.getId());
					} catch (Exception e) {

					}
				}
			}
		}

		// main code logic.

		// close things.
	}
}
