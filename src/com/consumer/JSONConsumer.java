package com.consumer;

import java.io.Reader;
import java.util.Collections;
import java.util.Properties;

import org.apache.ibatis.io.Resources;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class JSONConsumer {
	private static final String TOPIC = "exam";

	public static void main(String[] args) {
		String resource = "resources/Link.properties";
		Properties properties = new Properties();
		KafkaConsumer<String, String> consumer = null;
		JSONParser jsonParser = new JSONParser();
//		String message = null;
		
		try {
			Reader reader = Resources.getResourceAsReader(resource);
			properties.load(reader);
			properties.put("group.id", "group01");
			
			consumer = new KafkaConsumer<>(properties);
			consumer.subscribe(Collections.singletonList(TOPIC));
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(1000000);
				for (ConsumerRecord<String, String> record : records) {
//					message = record.value();
//					if(properties.getProperty("ExitMessage").equals(message)) System.exit(0);
					JSONObject jsonObj= (JSONObject) jsonParser.parse(record.value());
					jsonObj.keySet().forEach(val -> System.out.printf("%s : %s\n", val, jsonObj.get(val)));
					System.out.println("");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
