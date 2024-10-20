package com.windowforsun.kafka.batch.consume.consumer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchAcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.windowforsun.kafka.batch.consume.event.InboundEvent;
import com.windowforsun.kafka.batch.consume.mapper.JsonMapper;
import com.windowforsun.kafka.batch.consume.properties.DemoProperties;
import com.windowforsun.kafka.batch.consume.service.DemoService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@Profile("test-single-batch-stable-test")
@RequiredArgsConstructor
public class SingleTopicBatchStableConsumer2 implements BatchAcknowledgingConsumerAwareMessageListener<String, String> {

	public static final AtomicInteger MESSAGE_COUNTER = new AtomicInteger(0);
	public static final AtomicInteger BATCH_COUNTER = new AtomicInteger(0);
	public static boolean shouldError = true;

	private final DemoService demoService;
	private final DemoProperties demoProperties;

	@Override
	@KafkaListener(topics = "inbound-topic-1", groupId = "single-topic-consumer-group", containerFactory = "kafkaListenerContainerFactory")
	public void onMessage(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment,
		Consumer<?, ?> consumer) {

		log.info("test !! batch count {}", records.size());

		BATCH_COUNTER.getAndIncrement();

		for(ConsumerRecord<String, String> record : records) {
			MESSAGE_COUNTER.getAndIncrement();
			String key = record.key();
			String payload = record.value();
			log.info("stable batch inbound message batch count: {}, message count: {}, key: {}, payload: {}", BATCH_COUNTER.get(), MESSAGE_COUNTER.get(), key, payload);

			InboundEvent event = JsonMapper.readFromJson(payload, InboundEvent.class);

			if(event.getId().contains("test") && this.shouldError) {
				this.shouldError = false;
				throw new RuntimeException("Enforced fail on test");
			}

			// acknowledgment.acknowledge();
			log.info("test !! current record offset : {} partition : {}", record.offset(), record.partition());
			consumer.commitSync(
				Map.of(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 2)));

			this.demoService.process(key, event);
		}

		acknowledgment.acknowledge();
	}
}
