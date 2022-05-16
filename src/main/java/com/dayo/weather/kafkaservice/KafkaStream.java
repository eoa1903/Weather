package com.dayo.weather.kafkaservice;

import com.dayo.weather.entity.Weather;
import com.dayo.weather.serializers.CustomDeserializer;
import com.dayo.weather.serializers.CustomSerializer;
import com.google.gson.JsonDeserializer;
import lombok.extern.log4j.Log4j2;
import netscape.javascript.JSObject;
import org.apache.commons.collections4.IterableMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.kafka.streams.StreamsConfig.*;

@Configuration
@EnableKafka
@EnableKafkaStreams
@Log4j2
public class KafkaStream {
    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfig() throws Exception{
        return new KafkaStreamsConfiguration(Map.of(
                APPLICATION_ID_CONFIG, "IceStreaming",
                BOOTSTRAP_SERVERS_CONFIG, "192.168.2.47:29092",
                DEFAULT_KEY_SERDE_CLASS_CONFIG, String.class,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest"
        ));
    }

    @Bean
    public KStream<String, Weather> kstream(StreamsBuilder builder)throws Exception{
        Map<String, Object> serdeProps = new HashMap<>();

        final Serializer weatherSerializer = new CustomSerializer();
        weatherSerializer.configure(serdeProps, false);

        final Deserializer weatherDeserializer = new CustomDeserializer();
        weatherDeserializer.configure(serdeProps, false);

        final Serde weatherSerde = Serdes.serdeFrom(weatherSerializer, weatherDeserializer);

        KStream <String,Weather>stream = builder.stream("weather-data", Consumed.with(Serdes.String(),weatherSerde).withTimestampExtractor(new WeatherTimeExtractor()));
        stream.groupByKey().windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(15)));
        stream.peek((key, value) -> System.out.println("Incoming record 1 - key " +key +" value " + value.json()));

        KStream<String,Object> storeObjMap = stream.flatMap((key, value) -> {
            List<KeyValue<String,Object>> result = new ArrayList<>();
            result.add(KeyValue.pair(value.json().toString(),value.json().getClass().getSimpleName()));
            //System.out.println(result.toString());
            return result;
        });                     //values -> values.json().toMap().keySet(),Named.as("Yes"));
        return stream;
    }
}
