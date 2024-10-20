package com.windowforsun.kafka.batch.consume;

import com.windowforsun.kafka.batch.consume.event.InboundEvent;

public class Util {
	public static String INBOUND_DATA = "event data";

	public static InboundEvent buildInboundEvent(String id) {
		return InboundEvent.builder()
			.id(id)
			.data(INBOUND_DATA)
			.build();
	}
}
