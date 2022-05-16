package com.dayo.weather;

import com.dayo.weather.entity.Weather;
import com.dayo.weather.kafkaservice.Consumer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@Log4j2
@SpringBootTest
class WeatherApplicationTests {
	ZoneId zoneId= ZoneId.of("UTC");

	@Autowired
	private KafkaTemplate<String, Weather> kafkaTemplate;
	//public CountDownLatch countDownLatch = new CountDownLatch(1);
	@Autowired
	private Consumer consumer;
	ProducerRecord<String, Weather> producerRecord;

	@Test
	public void contextLoads() throws InterruptedException {
		int i = 0;

		while (true) {
			producerRecord = new ProducerRecord<>("weather-data", 0,"Group[1]", new Weather(i, "Rainfall", 234.56, 45.67, Instant.now().atZone(zoneId).toInstant().toEpochMilli()));
			ListenableFuture<SendResult<String, Weather>> future = kafkaTemplate.send(producerRecord);
			i++;
			future.addCallback(new ListenableFutureCallback<SendResult<String, Weather>>() {

				@Override
				public void onSuccess(SendResult<String, Weather> result) {
					final RecordMetadata m = result.getRecordMetadata();
					//System.out.printf("Sent message: Topic {%s} : Partition: {%s} : Offset: {%s} size: {%s}\n",m.topic(),m.partition(),m.offset(), m.serializedValueSize());
				}

				@Override
				public void onFailure(Throwable ex) {
					//System.out.println("Failed to send message");
					//System.out.println("Unable to send message=[" + message + "] due to : " + ex.getMessage());
				}
			});

			//assertThat(this.consumer.countDownLatch0.await(60,TimeUnit.SECONDS)).isTrue();

		}

	}

}
