package com.windowforsun.kafka.batch.consume.util;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import com.windowforsun.kafka.batch.consume.properties.DemoProperties;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaClient {
	private final DemoProperties demoProperties;
	private final KafkaTemplate<String, String> kafkaTemplate;

	public SendResult sendMessage(String key, String data) {
		try {
			String payload = "payload: " + data;
			// final ProducerRecord<String, String> record = new ProducerRecord<>(this.demoProperties.getOutboundTopic(),
			final ProducerRecord<String, String> record = new ProducerRecord<>("outbound-topic",
				key, payload);
			final SendResult result = (SendResult) this.kafkaTemplate.send(record).get();
			final RecordMetadata metadata = result.getRecordMetadata();


			log.info(String.format("Sent record(key=%s value=%s) meta(topic=%s, partition=%d, offset=%d)",
				record.key(), record.value(), metadata.topic(), metadata.partition(), metadata.offset()));

			return result;

		} catch (Exception e) {
			String message = "Error sending message to topic " + this.demoProperties.getOutboundTopic();
			log.error(message);
			throw new RuntimeException(message, e);
		}
	}


	// public SendResult sendMessage(String topic, String key, String data) {
	// 	try {
	// 		String payload = "payload: " + data;
	// 		final ProducerRecord<String, String> record = new ProducerRecord<>(topic,
	// 			key, payload);
	// 		final SendResult result = (SendResult) this.kafkaTemplate.send(record).get();
	// 		final RecordMetadata metadata = result.getRecordMetadata();
	//
	//
	// 		log.debug(String.format("Sent record(key=%s value=%s) meta(topic=%s, partition=%d, offset=%d)",
	// 			record.key(), record.value(), metadata.topic(), metadata.partition(), metadata.offset()));
	//
	// 		return result;
	//
	// 	} catch (Exception e) {
	// 		String message = "Error sending message to topic " + topic;
	// 		log.error(message);
	// 		throw new RuntimeException(message, e);
	// 	}
	// }
}
