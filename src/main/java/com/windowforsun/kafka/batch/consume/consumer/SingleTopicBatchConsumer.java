package com.windowforsun.kafka.batch.consume.consumer;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import com.windowforsun.kafka.batch.consume.event.InboundEvent;
import com.windowforsun.kafka.batch.consume.mapper.JsonMapper;
import com.windowforsun.kafka.batch.consume.properties.DemoProperties;
import com.windowforsun.kafka.batch.consume.service.DemoService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@Profile("test-single-batch")
@RequiredArgsConstructor
public class SingleTopicBatchConsumer {
	public static final AtomicInteger MESSAGE_COUNTER = new AtomicInteger(0);
	public static final AtomicInteger BATCH_COUNTER = new AtomicInteger(0);
	public static boolean shouldError = true;

	private final DemoService demoService;
	private final DemoProperties demoProperties;

	@KafkaListener(topics = "inbound-topic-1", groupId = "single-topic-consumer-group", containerFactory = "batchKafkaListenerContainerFactory")
	public void listen(@Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) List<String> keyList,
		@Payload List<String> payloadList) {

		BATCH_COUNTER.getAndIncrement();

		for(int i = 0; i < payloadList.size(); i++) {
			MESSAGE_COUNTER.getAndIncrement();
			String key = keyList.get(i);
			String payload = payloadList.get(i);
			log.info("inbound message batch count: {}, message count: {}, key: {}, payload: {}", BATCH_COUNTER.get(), MESSAGE_COUNTER.get(), key, payload);

			InboundEvent event = JsonMapper.readFromJson(payload, InboundEvent.class);

			if(event.getId().contains("test") && this.shouldError) {
				this.shouldError = false;

				throw new RuntimeException("Enforced fail on test");
			}

			this.demoService.process(key, event);
		}
	}
}
