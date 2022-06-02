package com.dayo.weather;

import com.dayo.weather.entity.Weather;
import com.dayo.weather.kafkaservice.Producer;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Log4j2
@SpringBootTest
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
					"\"timestamp\":" + System.currentTimeMillis() + "}");
			kafkaTemplate.send(record);
			i++;
		}
	}
}
