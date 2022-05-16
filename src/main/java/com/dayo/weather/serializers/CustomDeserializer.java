package com.dayo.weather.serializers;

import java.util.Map;

import com.dayo.weather.entity.Weather;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CustomDeserializer implements Deserializer<Weather> {
    private ObjectMapper objectMapper = new ObjectMapper();
    public CustomDeserializer(){}
    //private Class<Weather> tClass;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
       // tClass = (Class<Weather>) configs.get("JsonPOJOClass");
    }

    @Override
    public Weather deserialize(String topic, byte[] bytes) {
        try {
            if (bytes == null){
                return null;
            }
            objectMapper.registerModule(new JavaTimeModule());
            return objectMapper.readValue(bytes, Weather.class);
        } catch (Exception e) {
            log.info("Error -> {}", e.getMessage());
            throw new SerializationException("Error when deserializing Byte to Json");
        }
        //return data;
    }

    @Override
    public void close() {
    }
}