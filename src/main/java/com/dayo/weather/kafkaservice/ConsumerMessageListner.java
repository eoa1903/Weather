package com.dayo.weather.kafkaservice;

import com.dayo.weather.config.ConsumerConfig;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
@Log4j2
public class ConsumerMessageListner {
    @Autowired
    private ConsumerConfig consumerConfig;
    @Autowired
    private ConsumerMessageHandler messageHandler;

    @EventListener(ApplicationReadyEvent.class)
    public void starter(){
        new Thread(this::run).start();
    }

    public void run(){
        try{
            KafkaConsumer<String,String>kafkaConsumer = consumerConfig.getKafkaConsumer();
            while(true){
                for(ConsumerRecord<String, String> record:kafkaConsumer.poll(Duration.ofMillis(100))) {
                    messageHandler.process(record.value(),"id_"+record.key());
                }
            }
        }
        catch (Exception e){
        }
    }
}
