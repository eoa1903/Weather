package com.dayo.weather;

import com.dayo.weather.entity.Weather;
import com.dayo.weather.kafkaservice.KafkaConsumerConfig;
import com.dayo.weather.kafkaservice.Producer;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.ZoneId;

@Log4j2
@SpringBootTest
class WeatherApplicationTests {
	ZoneId zoneId= ZoneId.of("UTC");

	@Autowired
	private KafkaTemplate<String, Weather> kafkaTemplate;
	@Autowired
	Producer producer;

	ProducerRecord<String, Weather> producerRecord;
	ProducerRecord<String,Weather> minR;
	ProducerRecord<String,Weather> hourR;

	@Test
	public void contextLoads() throws InterruptedException {
		//Thread t1 = new Thread(producer);

		KafkaConsumerConfig consumers = new KafkaConsumerConfig ("weather-data");
		try {
			//consumers.init(2);
			//t1.start();

		}catch (Exception exp) {
			consumers.shutdown();
		}

//		//consumers.execute(10);
//
//		while (i<10000) {
//			producerRecord = new ProducerRecord<>("weather-data", 0,"1", new Weather(i, "Rainfall", 234.56, 45.67, Instant.now().atZone(zoneId).toInstant().toEpochMilli()));
//			i++;
//			minR = new ProducerRecord<>("weather-data", 0,"2", new Weather(i, "Rainfall", 2167.66, 21.63, Instant.now().atZone(zoneId).toInstant().toEpochMilli()));
//			i++;
//			hourR = new ProducerRecord<>("weather-data", 0,"3", new Weather(i, "Rainfall", 312.86, 09.71, Instant.now().atZone(zoneId).toInstant().toEpochMilli()));
//			kafkaTemplate.send(producerRecord);
//			kafkaTemplate.send(hourR);
//			kafkaTemplate.send(minR);
//			i++;
//		}


	}

}
