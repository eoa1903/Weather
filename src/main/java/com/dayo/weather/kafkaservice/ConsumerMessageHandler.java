package com.dayo.weather.kafkaservice;

import com.dayo.weather.entity.Weather;
import com.dayo.weather.process.MessageProcessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Service
@Log4j2
public class ConsumerMessageHandler {
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private MessageProcessor messageProcessor;

    @Async("threadExecutor")
    public void process(String data,String feedID){
        try {
            Weather weather = objectMapper.readValue(data, Weather.class);
            messageProcessor.process(weather,feedID);
        }
        catch (Exception e){
            log.warn("Unable to process message: " + data, e);
        }
    }
}
