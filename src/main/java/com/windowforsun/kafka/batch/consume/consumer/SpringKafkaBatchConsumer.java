package com.windowforsun.kafka.batch.consume.consumer;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.windowforsun.kafka.batch.consume.event.InboundEvent;
import com.windowforsun.kafka.batch.consume.mapper.JsonMapper;
import com.windowforsun.kafka.batch.consume.service.DemoService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@Profile("test-spring-kafka-batch")
@RequiredArgsConstructor
public class SpringKafkaBatchConsumer {
	public static final AtomicInteger MESSAGE_COUNTER = new AtomicInteger(0);
	public static final AtomicInteger BATCH_COUNTER = new AtomicInteger(0);
	public static boolean shouldError = true;

	private final DemoService demoService;

	@KafkaListener(topics = "inbound-topic-1", groupId = "spring-kafka-batch-consumer-group", containerFactory = "kafkaListenerContainerFactory")
	public void listen(ConsumerRecords<String, String> records) {
		BATCH_COUNTER.getAndIncrement();

		for(ConsumerRecord<String, String> record : records) {
			MESSAGE_COUNTER.getAndIncrement();
			String key = record.key();
			String payload = record.value();
			log.info("inbound message message count: {}, key: {}, payload: {}", MESSAGE_COUNTER.get(), key, payload);

			InboundEvent event = JsonMapper.readFromJson(payload, InboundEvent.class);

			if(event.getId().contains("test") && this.shouldError) {
				this.shouldError = false;
				throw new RuntimeException("Enforced fail on test");
			}

			this.demoService.process(key, event);
		}
	}
}
