package com.windowforsun.kafka.batch.consume.service;

import org.springframework.stereotype.Service;

import com.windowforsun.kafka.batch.consume.event.InboundEvent;
import com.windowforsun.kafka.batch.consume.util.KafkaClient;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class DemoService {
	private final KafkaClient kafkaClient;

	public void process(String key, InboundEvent event) {
		this.kafkaClient.sendMessage(key, event.getData());
	}
}
