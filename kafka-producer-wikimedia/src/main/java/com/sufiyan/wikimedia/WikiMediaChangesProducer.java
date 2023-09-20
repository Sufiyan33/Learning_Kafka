package com.sufiyan.wikimedia;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;

public class WikiMediaChangesProducer {

	/*
	 * Writing producer to send data which are coming from stream to consumer.
	 */
	public static void main(String[] args) throws InterruptedException {

		// Create producer properties.
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// Create producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		String topic = "wikimedia.recentchange";

		// Now create EventHandler to trigger event.
		EventHandler eventhandler = new WikimediaChangeHandler(producer, topic);
		String url = "https://stream.wikimedia.org/v2/stream/recentchange";

		EventSource.Builder builder = new EventSource.Builder(eventhandler, URI.create(url));
		EventSource eventsource = builder.build();

		// Start the producer in another thread.
		eventsource.start();

		// we produce for 10 minutes and block the program until then.
		TimeUnit.MINUTES.sleep(10);
	}
}
