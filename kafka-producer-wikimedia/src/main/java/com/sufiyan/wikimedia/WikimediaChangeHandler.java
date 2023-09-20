package com.sufiyan.wikimedia;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;

public class WikimediaChangeHandler implements EventHandler {

	KafkaProducer<String, String> kafkaProducer;
	String topic;
	private final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

	public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
		this.kafkaProducer = kafkaProducer;
		this.topic = topic;
	}

	public void onOpen() {
		// Do nothing here.
	}

	public void onClosed() {
		kafkaProducer.close();
	}

	public void onMessage(String event, MessageEvent messageEvent) {

		log.info(messageEvent.getData());

		// Send event/data with producer record.
		kafkaProducer.send(new ProducerRecord<String, String>(topic, messageEvent.getData()));
	}

	public void onComment(String comment) {

		// nothing here
	}

	public void onError(Throwable t) {

		log.error("Error in streaming data😒");
	}

}
