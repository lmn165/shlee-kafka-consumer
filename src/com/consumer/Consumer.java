package com.consumer;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Consumer {
	private static final String TOPIC = "exam";
	private static final String SERVERS = "localhost:9092";
	private static final String EXIT_MESSAGE = "exit";

	public static void main(String[] args) {
		Properties prop = new Properties();
		prop.put("bootstrap.servers", SERVERS);
		prop.put("group.id", "group01");
		prop.put("enable.auto.commit", "true");
		prop.put("auto.commit.interval.ms", "1000");
		prop.put("session.timeout.ms", "30000");

		prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, String> consumer = null;
		String message = null;

		try {
			consumer = new KafkaConsumer<>(prop);
			consumer.subscribe(Collections.singletonList(TOPIC));
			do {
				ConsumerRecords<String, String> records = consumer.poll(1000000);

				for (ConsumerRecord<String, String> record : records) {
					message = record.value();
					System.out.println(message);
				}
			} while (!EXIT_MESSAGE.equals(message));
		} catch (Exception e) {
			System.out.println("exception : " + e);
		}
	}
}
