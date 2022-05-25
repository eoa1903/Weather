package com.dayo.weather.kafkaservice;

import com.dayo.weather.entity.Weather;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.ZoneId;

@Log4j2
@Service
public class Producer implements Runnable{
    @Autowired
    KafkaTemplate<String, Weather> kafkaTemplate;
    ZoneId zoneId= ZoneId.of("UTC");

    @Override
    public void run() {
        int i = 0;
        while (true) {
            sendToTopic(new Weather(i, "Rainfall", 234.56, 45.67, Instant.now().atZone(zoneId).toInstant().toEpochMilli()), "weather-data", 0,"1");
            i++;
            sendToTopic(new Weather(i, "Rainfall", 234.56, 45.67, Instant.now().atZone(zoneId).toInstant().toEpochMilli()), "weather-data", 0,"2");
            i++;
            sendToTopic(new Weather(i, "Rainfall", 234.56, 45.67, Instant.now().atZone(zoneId).toInstant().toEpochMilli()), "weather-data", 0,"3");
            i++;
            sendToTopic(new Weather(i, "Rainfall", 234.56, 45.67, Instant.now().atZone(zoneId).toInstant().toEpochMilli()), "weather-data", 0,"4");
            i++;
            sendToTopic(new Weather(i, "Rainfall", 234.56, 45.67, Instant.now().atZone(zoneId).toInstant().toEpochMilli()), "weather-data", 0,"5");
            i++;
            sendToTopic(new Weather(i, "Rainfall", 234.56, 45.67, Instant.now().atZone(zoneId).toInstant().toEpochMilli()), "weather-data", 0,"6");
            i++;
        }
    }

    /**
     *
     * @param data Weather object
     * @param topic the topic name
     */

    public void sendToTopic(Weather data, String topic, int partiton, String key){
        final ProducerRecord<String, Weather> producerRecord = new ProducerRecord<>(topic,partiton,key,data); //create producer record
        kafkaTemplate.send(producerRecord);
    }
}
