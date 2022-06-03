package com.dayo.weather;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;


@Log4j2
@SpringBootTest
@ComponentScan(basePackages = {"com.dayo.weather.*"})
@EntityScan(basePackages = {"com.dayo.weather.*"})
class WeatherApplicationTests {
	@Autowired
	KafkaTemplate<String,String>kafkaTemplate;

	@Test
	public void contextLoads() throws InterruptedException {

		int i = 0;
		while (true) {
			ProducerRecord<String, String> record = new ProducerRecord<>("weather-data", 0, "1", "{\"id\":" + i + ",\n" +
					"\"phyQt\":\"Rainfall\",\n" +
					"\"lon\":" + 23.5 + ",\n" +
					"\"lat\":" + 2.89 + ",\n" +
					"\"timestamp\":" + ZonedDateTime.ofInstant(Instant.now(),ZoneId.of("UTC")).toInstant().toEpochMilli() + "}");
			kafkaTemplate.send(record);
			i++;
		}
	}
}
