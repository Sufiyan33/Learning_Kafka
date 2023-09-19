package com.sufiyan.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoKeys {
	/*
	 * Here we will create producer with callback and some properties but data will
	 * be send a long with Keys. I will use conductor platform to visualize topics,
	 * partitions and offsets. Hence I will use same credentials to connect.
	 * 
	 * Important : Same keys will go in same partitions.
	 */
	// Approaches
	/*
	 * Step 1 : Create Producer properties
	 * 
	 * Step 2 : Create Producer.
	 * 
	 * Step 3 : while sending data pass your callback method means after
	 * successfully sending data you want to print timeStamp, id or metadata.
	 * 
	 * Step 4 : flush and close the producer.
	 */

	private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

	public static void main(String[] args) throws InterruptedException {
		log.info("This is a producer with callback class !!!");

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

		// Send more messages and set sleep time to demonstrate StickyPartitionar.
		for (int j = 0; j < 2; j++) {
			// Now send multiple messages at a time to a topic
			for (int i = 0; i < 10; i++) {

				// Create here, keys and values.
				String topic = "demo_java";
				final String key = "id_" + i;
				String value = "Hello producer with keys 😊!!! " + i;
				ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, key, value);

				// while sending data add callBack.
				producer.send(producerRecord, new Callback() {

					public void onCompletion(RecordMetadata metadata, Exception e) {
						// Executes every time a record successfully sent or exception is thrown.
						if (e == null) {
							// means the record successfully sent.
							log.info("Key: " + key + " | Partition: " + metadata.partition());
						} else {
							log.error("Error while producing " + e);
						}

					}
				});

				Thread.sleep(500);
			}
		}

		// tell the producer to send all data and block until done --synchronous way.
		producer.flush();

		// flush and close the producer.
		producer.close();

	}
}
