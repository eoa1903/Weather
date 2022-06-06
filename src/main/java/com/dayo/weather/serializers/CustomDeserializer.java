package com.dayo.weather.serializers;

import java.util.Map;

import com.dayo.weather.entity.Weather;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor
public class CustomDeserializer implements Deserializer<Weather> {
    private ObjectMapper objectMapper = new ObjectMapper();
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