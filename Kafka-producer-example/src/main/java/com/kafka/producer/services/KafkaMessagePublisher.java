package com.kafka.producer.services;

import java.util.concurrent.CompletableFuture;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import com.kafka.producer.modal.Employee;

@Service
public class KafkaMessagePublisher {

	@Autowired
	private KafkaTemplate<String, Object> template;

	public void sendMessageToTopic(String message) {
		CompletableFuture<SendResult<String, Object>> future = template.send("kafka-demo-5", message);
		// to make this call asynchronous use below :
		// Here we can also print partition number as well along with offset

		future.whenComplete((result, exceptions) -> {
			if (exceptions == null) {
				System.out.println("Sent message = [" + message + "] with offSeet = [ "
						+ result.getRecordMetadata().offset() + "]");
			} else {
				System.out.println("Unable to sent message [" + message + "] due to : " + exceptions.getMessage());
			}
		});
	}

	public void sendEmployeeDataToTopic(Employee employee) {
		try {
			CompletableFuture<SendResult<String, Object>> future = template.send("Employee-topics", employee);
			// to make this call asynchronous use below :
			// Here we can also print partition number as well along with offset

			future.whenComplete((result, exceptions) -> {
				if (exceptions == null) {
					System.out.println("Sent message = [" + employee.toString() + "] with offSeet = [ "
							+ result.getRecordMetadata().offset() + "]");
				} else {
					System.out.println(
							"Unable to sent message [" + employee.toString() + "] due to : " + exceptions.getMessage());
				}
			});
		} catch (Exception e) {
			System.out.println("Exception came :: " + e.getMessage());
		}
	}
}
