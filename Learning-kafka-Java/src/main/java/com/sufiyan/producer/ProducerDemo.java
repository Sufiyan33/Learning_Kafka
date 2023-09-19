package com.sufiyan.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemo {
	/*
	 * Here we will create producer and some properties. I will use conductor
	 * platform to visualize topics, partitions and offsets. Hence I will use same
	 * credentials to connect.
	 */
	// Approaches
	/*
	 * Step 1 : Create Producer properties
	 * 
	 * Step 2 : Create Producer.
	 * 
	 * Step 3 : Send data.
	 * 
	 * Step 4 : flush and close the producer.
	 */

	private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

	public static void main(String[] args) {
		log.info("This is a producer class !!!");

		// Create Producer properties
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

		// Set producer properties
		properties.setProperty("key.serializer", StringSerializer.class.getName());
		properties.setProperty("value.serializer", StringSerializer.class.getName());

		// Create Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		/*
		 * Now create a producer record to send by using producer with topic name and
		 * message that want to send.
		 * 
		 * Now you have to create topic with name "demo_java". For this either use
		 * conduktor UI or cli
		 */

		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("demo_java",
				"Hello world 😊!!!");

		// Send data.
		producer.send(producerRecord);
		// tell the producer to send all data and block until done --synchronous way.
		producer.flush();

		// flush and close the producer.
		producer.close();

	}
}
