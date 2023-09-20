package com.sufiyan.producer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoCooperative {
	/*
	 * Here, we will show different types of strategy available to re-balance
	 * consumers.
	 * 
	 * There are 3 strategy which comes under Eager Re-balancing. If we are using :
	 * 
	 * 1 : kafka-consumer then partition.assignment.strategy :
	 * 
	 * 1- RangeAssignor 2- RoundRobin 3- StickyAssignor.
	 * 
	 * 2 : Kafka connect - Already implemented Co-operativeStickyAssignor.
	 * 
	 * 3 : Kafka streams - turned on by default using streamPartitionAssignor.
	 * 
	 * Only you need to set property.
	 * 
	 */

	private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());

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

		// Set re-balancing strategy.
		properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

		// Strategy for making static member.
		// properties.setProperty("group.instance.id", ".....");

		// Create Consumer
		final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

		// Get main thread reference here.
		final Thread mainThread = Thread.currentThread();

		// Adding shutdown hook.
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				log.info("Detected a shoutdown, Let's exit by calling consumer.wakeup()....");
				consumer.wakeup();

				// Join the main thread to allow the execution of the code in main thread.
				try {
					mainThread.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});

		try {
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
		} catch (WakeupException e) {
			log.info("Consumer is starting to shutdown");
		} catch (Exception e) {
			log.error("Unexpected exception in the consumer ", e);
		} finally {
			consumer.close(); // close the consumer, This will also commit offset.
			log.info("The consumer is now gracefully shutdown😊");
		}
	}
}
