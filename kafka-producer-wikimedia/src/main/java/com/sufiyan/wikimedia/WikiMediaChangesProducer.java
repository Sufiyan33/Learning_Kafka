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

		// Set safe producer config(kafka version <= 2.8)
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		properties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // same as setting -1.
		properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

		// Set high throughput producer configs.
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

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
