package com.windowforsun.kafka.batch.consume;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

@TestConfiguration
public class TestConfig {
	private static final Logger log = LoggerFactory.getLogger(TestConfig.class);
	@Value("${kafka.bootstrap-servers}")
	private String bootstrapServers;

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> testKafkaListenerContainerFactory(final ConsumerFactory<String, String> testConsumerFactory) {
		final ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(testConsumerFactory);

		return factory;
	}

	@Bean
	public ConsumerFactory<String, String> testConsumerFactory() {
		final Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1000000);
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
		props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 1000);

		return new DefaultKafkaConsumerFactory<>(props);
	}

	@Bean
	public ProducerFactory<String, Object> testProducerFactory() {
		final Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
		props.put(ProducerConfig.LINGER_MS_CONFIG, 300);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		props.put(ProducerConfig.RETRIES_CONFIG, 0);

		return new DefaultKafkaProducerFactory<>(props);
	}
	@Bean
	public KafkaTemplate<String, Object> testKafkaTemplate(final ProducerFactory<String, Object> testProducerFactory) {
		return new KafkaTemplate<>(testProducerFactory);
	}

	@Bean
	public KafkaTestListener testKafkaListener() {
		return new KafkaTestListener();
	}

	public static class KafkaTestListener {
		public static AtomicInteger counter = new AtomicInteger(0);
		public static List<String> receivedKeyList = new ArrayList<>();

		@KafkaListener(groupId = "testListener", topics = "outbound-topic", autoStartup = "true", containerFactory = "testKafkaListenerContainerFactory")
		void receive(@Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key, @Payload String payload) {
			log.info("outbound topic count: {}, key: {}, payload: {}", counter.incrementAndGet(), key, payload);
			receivedKeyList.add(key);
		}
	}
}
