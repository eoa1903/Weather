package com.dayo.weather;

import com.dayo.weather.entity.Weather;
import com.dayo.weather.kafkaservice.KafkaConsumerConfig;
import com.dayo.weather.kafkaservice.Producer;
import lombok.extern.log4j.Log4j2;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Log4j2
@SpringBootTest
class WeatherApplicationTests {
	ZoneId zoneId= ZoneId.of("UTC");

	@Autowired
	private KafkaTemplate<String, Weather> kafkaTemplate;
	@Autowired
	Producer producer;

	@Test
	public void contextLoads() throws InterruptedException {
		Thread t1 = new Thread(producer);
		t1.start();

		KafkaConsumerConfig consumers = new KafkaConsumerConfig();
		try{
			consumers.init(20);

		}catch (Exception exp) {
			//consumers.shutdown();
		}


	}
//	@Test
//	public void checkMax(){
//		assertEquals(2, Math.max(1, 2));
//	}
//
}
