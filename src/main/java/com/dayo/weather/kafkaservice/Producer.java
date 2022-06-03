package com.dayo.weather.kafkaservice;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Log4j2
@Service
public class Producer {
    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    /**
     *
     * @param data Weather object
     * @param topic the topic name
     */
    public void sendToTopic(String data, String topic, int partition, String key){
        final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic,partition,key,data); //create producer record
        kafkaTemplate.send(producerRecord);
    }
}
