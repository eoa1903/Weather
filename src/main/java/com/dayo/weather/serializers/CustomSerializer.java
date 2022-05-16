package com.dayo.weather.serializers;

import com.dayo.weather.entity.Weather;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

@Slf4j
public class CustomSerializer implements Serializer<Weather> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Weather data) {
        try {
            if(data ==null) {
                return null;
            }
            objectMapper.registerModule(new JavaTimeModule());
            return objectMapper.writeValueAsBytes(data);
        }
        catch(Exception e) {
            throw new SerializationException("Error when serializing Json to Byte");
        }
    }

    @Override
    public void close() {
    }

}

