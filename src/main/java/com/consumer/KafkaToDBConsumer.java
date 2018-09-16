package com.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.configuration.SimpleJsonSerializer;
import com.model.Message;
import com.writer.JdbcBatchWriter;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Component
public class KafkaToDBConsumer {

	@Autowired
	@Getter
	@Setter
	@Qualifier("outputDataSource")
	private DataSource outputDataSource;

	@Autowired
	@Getter
	@Setter
	@Qualifier("jdbcBatchWriter")
	private JdbcBatchWriter writer;

	@Value("${kafka.topic.key}")
	private String key;

	private AtomicInteger count = new AtomicInteger();

	final ObjectMapper oMapper = new ObjectMapper();

	@Autowired
	SimpleJsonSerializer jsonserializer;

	@KafkaListener(id = "${group.id}", topics = "${kafka.consumer.subscribe.topics}", containerFactory = "batchKafkaListenerContainerFactory")
	public void receive(List<ConsumerRecord<String, String>> payloads) {
		processMessage(payloads);
	}

	/**
	 * process message from kafka and call process for level 1 and other level
	 * 
	 * @param payloads
	 * 
	 */
	private void processMessage(List<ConsumerRecord<String, String>> payload) {
		String topic = null;
		int partition = -1;
		long initialOffset = -1L;
		long finalOffset = -1L;
		try {
			List<ConsumerRecord<String, String>> payloads = payload.stream().filter(e -> key.equals(e.key()))
					.collect(Collectors.toList());

			// log basic processing information about topic, partition and
			// offset
			if (!payloads.isEmpty() && payloads.get(0) != null) {
				topic = payloads.get(0).topic();
				partition = payloads.get(0).partition();
				initialOffset = payloads.get(0).offset();
				log.info("processing will start with - : topic : {} , partition : {} , offset : {} ", topic, partition,
						initialOffset);
				finalOffset = payloads.get(payloads.size() - 1).offset();
				log.info("processing will end with - : topic : {} , partition : {} , offset : {} ", topic, partition,
						finalOffset);
			}

			List<Map<String, Object>> itemsToWrite = new ArrayList<>();
			// create list of messages
			for (int i = 0; i < payloads.size(); i++) {
				// validate if data can be casted to Message
				Message message = jsonserializer.fromJson(payloads.get(i).value(), Message.class);
				if (message != null) {
					Map<String, Object> m = oMapper.convertValue(message, Map.class);
					itemsToWrite.add(m);
				}
			}
			writer.write(itemsToWrite, initialOffset, finalOffset, topic, partition);
		} catch (Exception e) {
			log.error("exception {} ", e);
		}
	}
}