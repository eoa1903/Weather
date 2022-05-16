package com.dayo.weather.kafkaservice;

import com.dayo.weather.entity.Weather;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Log4j2
@Service
public class Producer {
    @Autowired
    KafkaTemplate<String, Weather> kafkaTemplate;

    /**
     *
     * @param data Weather object
     * @param topic the topic name
     */
    public void sendToTopic(Weather data, String topic, int partiton){
        final ProducerRecord<String, Weather> producerRecord = new ProducerRecord<>(topic,partiton,String.valueOf(data.getId()),data); //create producer record
        log.info("Producer timeStamp -> {}",producerRecord.timestamp());
        kafkaTemplate.send(producerRecord).addCallback( result -> { //send all the record and wait for callback for success
                    final RecordMetadata m;
                    if (result != null) {
                        m = result.getRecordMetadata();
                        log.info("Produced record to topic = {} partition = {} @offset = {}",
                                m.topic(),
                                m.partition(),
                                m.offset());
                    }
                },
                exception -> log.error("Failed to produce to kafka", exception));

        kafkaTemplate.flush();      //force all message in send queue to be delivered to server
    }
}
