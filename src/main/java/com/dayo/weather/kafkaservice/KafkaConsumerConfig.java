package com.dayo.weather.kafkaservice;

import com.dayo.weather.entity.Weather;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Component;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.providers.PooledConnectionProvider;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@EnableKafka
@Component
@Log4j2
public class KafkaConsumerConfig {

    private KafkaConsumer<String, Weather> consumer;
    private ExecutorService executor;

    public KafkaConsumerConfig() {
        this.consumer = new KafkaConsumer<>(consumerConfig());
        this.consumer.subscribe(Arrays.asList("weather-data"));
    }

    public void init(int numberOfThreads) {

        executor = new ThreadPoolExecutor(numberOfThreads, 256, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(), new ThreadPoolExecutor.CallerRunsPolicy());

        while (true) {
            ConsumerRecords<String, Weather> records = consumer.poll(Duration.ofMillis(100));
            executor.submit(new ConsumerThreadHandler(records));

        }
    }

    public Map<String, Object> consumerConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.2.47:29092");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);    //1000 polls
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 150000);
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