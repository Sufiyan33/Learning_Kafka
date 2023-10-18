package com.kafka.producer.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafka.producer.modal.Employee;
import com.kafka.producer.services.KafkaMessagePublisher;

@RestController
@RequestMapping("/producer")
public class EventController {

	@Autowired
	private KafkaMessagePublisher publisher;

	@GetMapping("publish/{message}")
	public ResponseEntity<?> publishMessage(@PathVariable String message) {
		try {
			for (int i = 0; i < 1000; i++) {
				publisher.sendMessageToTopic(message + "-" + i);
			}
			return ResponseEntity.ok("Message has been published successfully");
		} catch (Exception e) {
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
		}
	}

	@PostMapping("/produce")
	public void sendEmployeeData(@RequestBody Employee employee) {
		publisher.sendEmployeeDataToTopic(employee);
	}
}
