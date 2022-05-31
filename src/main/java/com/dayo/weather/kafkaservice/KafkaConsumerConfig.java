package com.dayo.weather.kafkaservice;

import com.dayo.weather.entity.Weather;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Component;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

@EnableKafka
@Component
public class KafkaConsumerConfig {

    private KafkaConsumer<String, Weather> consumer;
    private ExecutorService executor;

    public KafkaConsumerConfig() {
        this.consumer = new KafkaConsumer<>(consumerConfig());
        this.consumer.subscribe(Arrays.asList("weather-data"));
    }

    public void init(int numberOfThreads) {
        executor = new ThreadPoolExecutor(numberOfThreads, Integer.MAX_VALUE, 0L, TimeUnit.MILLISECONDS,
                new SynchronousQueue<>(), new ThreadPoolExecutor.CallerRunsPolicy());

        while (true) {
            ConsumerRecords<String, Weather> records = consumer.poll(Duration.ofMillis(1000));
            if (!records.isEmpty()) {
                executor.submit(new ConsumerThreadHandler(records));
            }
        }
    }

    public Map<String, Object> consumerConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.2.47:29092");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10000);    //10000 polls
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1000000);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "weatherSubscriber");
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 1000);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 500);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        return props;
    }
}