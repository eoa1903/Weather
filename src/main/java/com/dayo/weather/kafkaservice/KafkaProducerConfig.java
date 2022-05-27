package com.dayo.weather.kafkaservice;

import com.dayo.weather.entity.Weather;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {
    @Bean
    public Map<String, Object> producerConfig(){
        Map<String,Object> props = new HashMap<>();
        props.put(ProducerConfig.ACKS_CONFIG,"all");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.2.47:29092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,"100000");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        return props;
    }

    @Bean
    public ProducerFactory<String, Weather> producerFactory(){
        return new DefaultKafkaProducerFactory<>(producerConfig());
    }

    @Bean
    public KafkaTemplate<String,Weather> kafkaTemplate(ProducerFactory<String, Weather> producerFactory){
        return new KafkaTemplate<>(producerFactory);
    }
}
