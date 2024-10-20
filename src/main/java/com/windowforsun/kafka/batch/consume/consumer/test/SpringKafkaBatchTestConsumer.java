package com.windowforsun.kafka.batch.consume.consumer.test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.windowforsun.kafka.batch.consume.event.InboundEvent;
import com.windowforsun.kafka.batch.consume.mapper.JsonMapper;
import com.windowforsun.kafka.batch.consume.properties.DemoProperties;
import com.windowforsun.kafka.batch.consume.service.DemoService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@Profile("test-single-batch-stable-1111")
@RequiredArgsConstructor
public class SpringKafkaBatchTestConsumer {
	public static final AtomicInteger MESSAGE_COUNTER = new AtomicInteger(0);
	public static final AtomicInteger BATCH_COUNTER = new AtomicInteger(0);
	public static boolean shouldError = true;

	private final DemoService demoService;
	private final DemoProperties demoProperties;

	// @KafkaListener(topics = "inbound-topic-1", groupId = "single-topic-consumer-group", containerFactory = "batchKafkaListenerContainerFactory")
	// @KafkaListener(topics = "inbound-topic-1", groupId = "single-topic-consumer-group", containerFactory = "stableBatchKafkaListenerContainerFactory")
	// public void listen(@Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) List<String> keyList,
	// 	@Payload List<String> payloadList,
	// 	// List<ConsumerRecord<String, String>> records,
	// 	ConsumerRecords<String, String> records,
	// 	Acknowledgment acknowledgment,
	// 	Consumer<String, String> consumer) {
	//
	// 	BATCH_COUNTER.getAndIncrement();
	// 	Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
	//
	// 	for(int i = 0; i < payloadList.size(); i++) {
	// 		MESSAGE_COUNTER.getAndIncrement();
	// 		String key = keyList.get(i);
	// 		String payload = payloadList.get(i);
	// 		ConsumerRecord<String, String> record = iterator.next();
	// 		log.info("stable batch inbound message batch count: {}, message count: {}, key: {}, payload: {}", BATCH_COUNTER.get(), MESSAGE_COUNTER.get(), key, payload);
	//
	// 		InboundEvent event = JsonMapper.readFromJson(payload, InboundEvent.class);
	// 		// acknowledgment.acknowledge();
	// 		// consumer.commitSync();
	//
	// 		if(event.getId().contains("test") && this.shouldError) {
	// 			this.shouldError = false;
	// 			throw new RuntimeException("Enforced fail on test");
	// 		}
	// 		// acknowledgment.acknowledge();
	// 		consumer.commitSync(Map.of(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1)));
	//
	// 		this.demoService.process(key, event);
	// 	}
	// }

	// @KafkaListener(topics = "inbound-topic-1", groupId = "single-topic-consumer-group", containerFactory = "stableBatchKafkaListenerContainerFactory")
	@KafkaListener(topics = "inbound-topic-1", groupId = "single-topic-consumer-group", containerFactory = "kafkaListenerContainerFactory")
	public void listen(
		// @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) List<String> keyList,
		// @Payload List<String> payloadList,
		// List<ConsumerRecord<String, String>> records,
		ConsumerRecords<String, String> records,
		Consumer<?, ?> consumer) {

		log.info("test !! batch count {}", records.count());

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
			consumer.commitSync(Map.of(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1)));

			this.demoService.process(key, event);
		}
	}

}
