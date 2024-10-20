package com.windowforsun.kafka.batch.consume.properties;

import java.util.List;

import javax.validation.constraints.NotNull;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Data;

@Data
@Configuration
@ConfigurationProperties("kafkademo")
public class DemoProperties {
	@NotNull
	private String outboundTopic;
	// @NotNull
	// private List<String> inboundTopics;
}
