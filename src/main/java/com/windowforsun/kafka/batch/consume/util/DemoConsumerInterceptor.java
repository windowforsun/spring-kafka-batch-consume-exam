package com.windowforsun.kafka.batch.consume.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DemoConsumerInterceptor implements ConsumerInterceptor<String, String> {
	private static final Logger log = LoggerFactory.getLogger(DemoConsumerInterceptor.class);
	public static AtomicInteger POLL_COUNT = new AtomicInteger(0);
	public static HashMap<Integer, List<String>> POLL_ITEMS = new HashMap<>();

	@Override
	public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> consumerRecords) {
		POLL_COUNT.getAndIncrement();
		List<String> pollList = new ArrayList<>();


		consumerRecords.forEach(stringStringConsumerRecord -> {
			String item = stringStringConsumerRecord.topic() + "::" + stringStringConsumerRecord.key();

			pollList.add(item);
		});

		log.info("poll count : {}, topic::key list : {}", consumerRecords.count(), pollList.stream().collect(Collectors.joining(", ")));

		POLL_ITEMS.put(POLL_COUNT.get(), pollList);
		return consumerRecords;
	}

	@Override
	public void onCommit(Map<TopicPartition, OffsetAndMetadata> map) {

	}

	@Override
	public void close() {

	}

	@Override
	public void configure(Map<java.lang.String, ?> map) {

	}
}
