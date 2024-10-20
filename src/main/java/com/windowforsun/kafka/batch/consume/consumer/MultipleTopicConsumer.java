package com.windowforsun.kafka.batch.consume.consumer;

import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import com.windowforsun.kafka.batch.consume.event.InboundEvent;
import com.windowforsun.kafka.batch.consume.mapper.JsonMapper;
import com.windowforsun.kafka.batch.consume.service.DemoService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@Profile("test-multiple")
@RequiredArgsConstructor
public class MultipleTopicConsumer {
	public static final AtomicInteger MESSAGE_COUNTER = new AtomicInteger(0);
	public static boolean shouldError = true;

	private final DemoService demoService;

	@KafkaListener(topics = {"inbound-topic-1", "inbound-topic-2", "inbound-topic-3"}, groupId = "multiple-topic-consumer-group", containerFactory = "kafkaListenerContainerFactory")
	public void listen(@Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
		@Payload String payload) {
		MESSAGE_COUNTER.getAndIncrement();
		log.info("inbound message message count: {}, key: {}, payload: {}", MESSAGE_COUNTER.get(), key, payload);

		InboundEvent event = JsonMapper.readFromJson(payload, InboundEvent.class);

		if(event.getId().contains("test") && this.shouldError) {
			this.shouldError = false;

			throw new RuntimeException("Enforced fail on test");
		}

		this.demoService.process(key, event);
	}
}
