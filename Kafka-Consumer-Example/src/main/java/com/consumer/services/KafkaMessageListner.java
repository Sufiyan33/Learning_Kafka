package com.consumer.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.consumer.modal.Employee;

@Service
public class KafkaMessageListner {

	Logger log = LoggerFactory.getLogger(KafkaMessageListner.class);

	@KafkaListener(topics = "Employee-topics", groupId = "employee-group")
	public void consumeEmployeeData(Employee employee) {
		log.info("consumer consume the messages {}", employee.toString());
	}
	
	/*
	 * Now here, I will create multiple consumer and bind them to a single group Id.
	 * Let see how data distributed across all the partitions and consumers.
	 * 
	 * Don't use such type approach in production or in real project.
	 */
	
	// I am commenting below method as I want to send Employee data.
	/*
	 * @KafkaListener(topics = "kafka-demo-5", groupId = "group") public void
	 * consumeTwo(String message) { log.info("consumer-2 consume the messages {}",
	 * message); }
	 * 
	 * @KafkaListener(topics = "kafka-demo-5", groupId = "group") public void
	 * consumeThree(String message) { log.info("consumer-3 consume the messages {}",
	 * message); }
	 * 
	 * @KafkaListener(topics = "kafka-demo-5", groupId = "group") public void
	 * consumeFour(String message) { log.info("consumer-4 consume the messages {}",
	 * message); }
	 */

}
