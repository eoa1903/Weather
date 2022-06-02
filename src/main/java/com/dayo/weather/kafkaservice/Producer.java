package com.dayo.weather.kafkaservice;

import com.dayo.weather.entity.Weather;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.ZoneId;

@Log4j2
@Service
public class Producer {
    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;
//    @EventListener(ApplicationReadyEvent.class)
//    void start(){
//        new Thread(this::run).start();
//    }

//    public void run() {
//        int i = 0;
//        while (true) {
////            sendToTopic("{\"id\":"+i+",\n" +
////                    "\"phyQt\":\"Rainfall\",\n" +
////                    "\"lon\":"+23.5+",\n" +
////                    "\"lat\":"+2.89+",\n" +
////                    "\"timestamp\":"+System.currentTimeMillis()+"}", "weather-data", 0,"1");
////            i++;
//            sendToTopic("Hello", "weather-data", 0,"1");
//        }
//    }

    /**
     *
     * @param data Weather object
     * @param topic the topic name
     */

    public void sendToTopic(String data, String topic, int partiton, String key){
        final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic,partiton,key,data); //create producer record
        kafkaTemplate.send(producerRecord);
    }
}
