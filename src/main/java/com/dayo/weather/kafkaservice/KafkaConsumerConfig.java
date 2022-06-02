//package com.dayo.weather.kafkaservice;
//
//import com.dayo.weather.entity.Weather;
//import org.apache.kafka.clients.consumer.*;
//import org.apache.kafka.common.serialization.StringDeserializer;
//import org.springframework.kafka.annotation.EnableKafka;
//import org.springframework.kafka.support.serializer.JsonDeserializer;
//import org.springframework.stereotype.Component;
//import java.time.Duration;
//import java.util.*;
//import java.util.concurrent.*;
//
//@EnableKafka
//@Component
//public class KafkaConsumerConfig {
//
//    private KafkaConsumer<String, Weather> consumer;
//    private ExecutorService executor;
//
//    public KafkaConsumerConfig() {
//        this.consumer = new KafkaConsumer<>(consumerConfig());
//        this.consumer.subscribe(Arrays.asList("weather-data"));
//    }
//
//    public void init(int numberOfThreads) {
//        executor = new ThreadPoolExecutor(numberOfThreads, Integer.MAX_VALUE, 0L, TimeUnit.MILLISECONDS,
//                new SynchronousQueue<>(), new ThreadPoolExecutor.CallerRunsPolicy());
//
//        while (true) {
//            ConsumerRecords<String, Weather> records = consumer.poll(Duration.ofMillis(100));  //Timeout
//            if (!records.isEmpty()) {
//                executor.submit(new ConsumerThreadHandler(records));
//            }
//        }
//    }
//
//}