package com.windowforsun.kafka.batch.consume;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.ContainerProperties;

import com.windowforsun.kafka.batch.consume.util.DemoConsumerInterceptor;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@EnableKafka
@Configuration
@ComponentScan(basePackages = "com.windowforsun.kafka")
@Profile("test-spring-kafka-batch")
public class SpringKafkaBatchConfig {
	@Value("${kafka.bootstrap-servers}")
	private String bootstrapServers;

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(final ConsumerFactory<String, String> consumerFactory,
		ConsumerAwareRebalanceListener consumerAwareRebalanceListener) {
		final ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory);
		factory.setBatchListener(true);
		factory.getContainerProperties().setConsumerRebalanceListener(consumerAwareRebalanceListener);
		return factory;
	}

	@Bean
	public KafkaTemplate<String, String> kafkaTemplate(final ProducerFactory<String, String> producerFactory) {
		return new KafkaTemplate<>(producerFactory);
	}

	@Bean
	public ConsumerFactory<String, String> consumerFactory() {
		final Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, DemoConsumerInterceptor.class.getName());
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

		return new DefaultKafkaConsumerFactory<>(props);
	}

	@Bean
	public ProducerFactory<String, String> producerFactory() {
		final Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

		return new DefaultKafkaProducerFactory<>(props);
	}

	@Bean
	public ConsumerAwareRebalanceListener consumerAwareRebalanceListener() {
		return new ConsumerAwareRebalanceListener() {
			@Override
			public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer,
				Collection<TopicPartition> partitions) {
				System.out.println("test !! before commit partitions " + partitions.stream()
					.map(TopicPartition::partition)
					.map(integer -> integer + "")
					.collect(
						Collectors.joining(",")));
			}

			@Override
			public void onPartitionsRevokedAfterCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
				System.out.println("test !! after commit partitions " + partitions.stream()
					.map(TopicPartition::partition)
					.map(integer -> integer + "")
					.collect(
						Collectors.joining(",")));
			}

			@Override
			public void onPartitionsLost(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
			}

			@Override
			public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
			}

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
			}

			@Override
			public void onPartitionsLost(Collection<TopicPartition> partitions) {
			}
		};
	}
}
