package com.sufiyan.producer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {
	/*
	 * Here, we will create kafka consumer and trying to fetch data from producer.
	 */
	// Approaches
	/*
	 * Step 1 : Create Consumer properties
	 * 
	 * Step 2 : Create Consumer.
	 * 
	 * Step 3 : Send data.
	 * 
	 * Step 4 : flush and close the producer.
	 */

	private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

	public static void main(String[] args) {
		log.info("I am a kafka Consumer 😊 !!!");

		String groupId = "my-java-application";
		String topic = "demo_java";
		// Create Consumer properties
		Properties properties = new Properties();

		/*
		 * Connect to local host :
		 * 
		 * If you are going to use for local then use below/
		 */
		properties.setProperty("bootsrap.servers", "127.0.0.1:9092");

		// Connect to Conduktor playground or Remote server.

		properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
		properties.setProperty("security.protocol", "SASL_SSL");
		properties.setProperty("sasl.jaas.config",
				"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"797f6NHDaSKhaxV0VLgFvN\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI3OTdmNk5IRGFTS2hheFYwVkxnRnZOIiwib3JnYW5pemF0aW9uSWQiOjc2NzUwLCJ1c2VySWQiOjg5Mjk3LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiIwZDI1YWJiNy1lYmQ3LTRlODktYTZiYS1hZThhZmVjZWJiNzgifX0.kNLhjyH_tTL9sYINbIh9sBkjz2oMWl-eNfjBESQRr74\";");
		properties.setProperty("sasl.mechanism", "PLAIN");

		// Set consumer config
		properties.setProperty("key.deserializer", StringDeserializer.class.getName());
		properties.setProperty("value.deserializer", StringDeserializer.class.getName());
		properties.setProperty("group.id", groupId);
		properties.setProperty("auto.offset.reset", "earliest");

		// Connect to Upstash playground

		/*
		 * properties.setProperty("bootstrap.servers",
		 * "exact-guinea-8977-eu2-kafka.upstash.io:9092");
		 * properties.setProperty("security.protocol", "SASL_SSL");
		 * properties.setProperty("sasl.jaas.config",
		 * "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"ZXhhY3QtZ3VpbmVhLTg5Nzck0mltKf531HjM6jcOM2CG9qpHS35Zfq8ZGV8HmUI\" password=\"MmFiOTg3ZjYtYzJkYS00NjA0LThlYmYtNTUxMTRjNzZkMzY1\";"
		 * ); properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
		 * 
		 * // Set consumer config properties.setProperty("key.deserializer",
		 * StringDeserializer.class.getName());
		 * properties.setProperty("value.deserializer",
		 * StringDeserializer.class.getName()); properties.setProperty("group.id",
		 * groupId); properties.setProperty("auto.offset.reset", "earliest");
		 */

		// Create Consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

		// Subscribe to a topic.
		consumer.subscribe(Arrays.asList(topic));

		// Poll for data
		while (true) {
			log.info("polling");
			/*
			 * Now create a Consumer record to consume by producer with topic name message
			 * that want to send.
			 */
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

			for (ConsumerRecord<String, String> record : records) {
				log.info("Key: " + record.key() + ", Value: " + record.value());
				log.info("Partion: " + record.partition() + ", Offset: " + record.offset());
			}
		}
	}
}
