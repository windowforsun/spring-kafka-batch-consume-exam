package com.windowforsun.kafka.batch.consume;

import static org.hamcrest.Matchers.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import com.windowforsun.kafka.batch.consume.consumer.SingleTopicConsumer;
import com.windowforsun.kafka.batch.consume.util.DemoConsumerInterceptor;

import lombok.extern.slf4j.Slf4j;

import static org.hamcrest.MatcherAssert.*;

@Slf4j
@SpringBootTest(classes = DemoConfig.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@ActiveProfiles("test-single")
@Import(TestConfig.class)
@EmbeddedKafka(controlledShutdown = true, topics = {"inbound-topic-1", "outbound-topic"}, partitions = 1)
public class SingleTopicConsumerTest {
	@Autowired
	private EmbeddedKafkaBroker embeddedKafkaBroker;
	@Autowired
	private KafkaListenerEndpointRegistry registry;
	@Autowired
	@Qualifier("testKafkaTemplate")
	private KafkaTemplate<String, Object> testKafkaTemplate;
	@Autowired
	private TestConfig.KafkaTestListener kafkaTestListener;

	@BeforeEach
	public void setUp() {
		this.registry.getListenerContainers()
			.stream()
			.forEach(container -> ContainerTestUtils.waitForAssignment(container, this.embeddedKafkaBroker.getPartitionsPerTopic()));

		DemoConsumerInterceptor.POLL_COUNT.set(0);
		DemoConsumerInterceptor.POLL_ITEMS = new HashMap<>();
		TestConfig.KafkaTestListener.counter.set(0);
		TestConfig.KafkaTestListener.receivedKeyList = new ArrayList<>();
	}

	@Test
	public void success_for_consume() throws ExecutionException, InterruptedException {
		SingleTopicConsumer.shouldError = false;
		this.testKafkaTemplate.send("inbound-topic-1", "1", Util.buildInboundEvent("1"));
		this.testKafkaTemplate.send("inbound-topic-1", "2-test", Util.buildInboundEvent("2-test"));
		this.testKafkaTemplate.send("inbound-topic-1", "3", Util.buildInboundEvent("3"));
		Thread.sleep(500);
		this.testKafkaTemplate.send("inbound-topic-1", "4", Util.buildInboundEvent("4"));


		Awaitility.await().atMost(5, TimeUnit.SECONDS)
			.pollDelay(100, TimeUnit.MILLISECONDS)
			.until(TestConfig.KafkaTestListener.counter::get, is(4));

		List<String> outboundReceivedKeyList = TestConfig.KafkaTestListener.receivedKeyList;

		assertThat(DemoConsumerInterceptor.POLL_COUNT.get(), is(2));
		assertThat(DemoConsumerInterceptor.POLL_ITEMS.get(1), containsInAnyOrder("inbound-topic-1::1",
			"inbound-topic-1::2-test",
			"inbound-topic-1::3"));
		assertThat(DemoConsumerInterceptor.POLL_ITEMS.get(2), containsInAnyOrder("inbound-topic-1::4"));
		assertThat(outboundReceivedKeyList, hasSize(4));
		assertThat(outboundReceivedKeyList.get(0), is("1"));
		assertThat(outboundReceivedKeyList.get(1), is("2-test"));
		assertThat(outboundReceivedKeyList.get(2), is("3"));
		assertThat(outboundReceivedKeyList.get(3), is("4"));
	}

	@Test
	public void consume_fail_on_testKey() throws Exception {
		this.testKafkaTemplate.send("inbound-topic-1", "1", Util.buildInboundEvent("1"));
		this.testKafkaTemplate.send("inbound-topic-1", "2-test", Util.buildInboundEvent("2-test"));
		this.testKafkaTemplate.send("inbound-topic-1", "3", Util.buildInboundEvent("3"));
		Thread.sleep(500);
		this.testKafkaTemplate.send("inbound-topic-1", "4", Util.buildInboundEvent("4"));

		Awaitility.await().atMost(5, TimeUnit.SECONDS)
			.pollDelay(100, TimeUnit.MILLISECONDS)
			.until(TestConfig.KafkaTestListener.counter::get, is(4));

		List<String> outboundReceivedKeyList = TestConfig.KafkaTestListener.receivedKeyList;

		assertThat(DemoConsumerInterceptor.POLL_COUNT.get(), is(2));
		assertThat(DemoConsumerInterceptor.POLL_ITEMS.get(1), containsInAnyOrder("inbound-topic-1::1",
			"inbound-topic-1::2-test",
			"inbound-topic-1::3"));
		assertThat(DemoConsumerInterceptor.POLL_ITEMS.get(2), containsInAnyOrder("inbound-topic-1::2-test",
			"inbound-topic-1::3",
			"inbound-topic-1::4"));
		assertThat(outboundReceivedKeyList, hasSize(4));
		assertThat(outboundReceivedKeyList.get(0), is("1"));
		assertThat(outboundReceivedKeyList.get(1), is("2-test"));
		assertThat(outboundReceivedKeyList.get(2), is("3"));
		assertThat(outboundReceivedKeyList.get(3), is("4"));
	}
}
