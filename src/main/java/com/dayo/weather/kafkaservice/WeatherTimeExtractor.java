package com.dayo.weather.kafkaservice;

import com.dayo.weather.entity.Weather;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class WeatherTimeExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long l) {
        Weather timestamp = (Weather) consumerRecord.value();
        //System.out.println("Extracting time {"+timestamp.getZoneDate()+" }");
        return 0;//timestamp.getZoneDate();
    }
}
