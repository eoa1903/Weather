package com.dayo.weather.kafkaservice;

import com.dayo.weather.entity.Weather;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@EnableKafka
@Component
@Log4j2
public class KafkaConsumerConfig {

    private  KafkaConsumer<String, Weather> consumer=null;// =new KafkaConsumer<>(consumerConfig());
    private ExecutorService executor;


    public KafkaConsumerConfig(){
        this.consumer = new KafkaConsumer<>(consumerConfig());
        this.consumer.subscribe(Arrays.asList("weather-data"));
    }
    public void init(int numberOfThreads) {

        executor = new ThreadPoolExecutor(numberOfThreads, numberOfThreads, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

        while (true) {
            ConsumerRecords<String, Weather> records = consumer.poll(Duration.ofMillis(100));
            for (final ConsumerRecord <String,Weather> record : records) {
                executor.submit(new ConsumerThreadHandler(record));
            }
        }
    }

    //@Bean
    public Map<String, Object> consumerConfig(){
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.2.47:29092");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);    //1000 polls
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 100000);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"weatherSubscriber");
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 1000);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,1000);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        return props;
    }
    public void shutdown() {
        if (consumer != null) {
            consumer.close();
        }
        if (executor != null) {
            executor.shutdown();
        }
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                System.out
                        .println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            System.out.println("Interrupted during shutdown, exiting uncleanly");
        }
    }

//    //@Bean
//    public ConsumerFactory<String, Weather> consumerFactory(){
//        return new DefaultKafkaConsumerFactory<>(consumerConfig());
//    }

//    @Bean
//    public KafkaListenerContainerFactory<?> kafkaListenerContainerFactory() {
//
//        ConcurrentKafkaListenerContainerFactory<String, Weather> factory =
//                new ConcurrentKafkaListenerContainerFactory<>();
//        factory.setConsumerFactory(consumerFactory());
//        factory.setBatchListener(true);
//        return factory;
//    }
}
