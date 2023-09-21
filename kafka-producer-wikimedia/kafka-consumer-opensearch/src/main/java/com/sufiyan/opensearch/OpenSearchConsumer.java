package com.sufiyan.opensearch;

import java.io.IOException;
import java.net.URI;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	public static void main(String[] args) throws IOException {
		Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

		// Step 1 :
		RestHighLevelClient openSearchClient = createOpenSearchClient();

		// We need to create index on openSeach if it's doesn't exist.
		try (openSearchClient) {
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
		}

		// create our kafka client.

		// main code logic.

		// close things.
	}
}
