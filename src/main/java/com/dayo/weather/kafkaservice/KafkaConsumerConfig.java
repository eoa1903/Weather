package com.dayo.weather.kafkaservice;

import com.dayo.weather.entity.Weather;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Field;
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
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@EnableKafka
@Component
@Log4j2
public class KafkaConsumerConfig {

    private KafkaConsumer<String, Weather> consumer;
    private ExecutorService executor;
    private final Map<TopicPartition, ConsumerThreadHandler> activeTasks = new HashMap<>();
    private final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private long lastCommitTime = System.currentTimeMillis();


    public KafkaConsumerConfig() {
        this.consumer = new KafkaConsumer<>(consumerConfig());
        this.consumer.subscribe(Arrays.asList("weather-data"));
    }

    public void init(int numberOfThreads) {

        executor = new ThreadPoolExecutor(numberOfThreads, 256, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(), new ThreadPoolExecutor.CallerRunsPolicy());

        while (true) {
            ConsumerRecords<String, Weather> records = consumer.poll(Duration.ofMillis(100));
            handleFetchedRecords(records);
            checkActiveTasks();
            commitOffsets();
        }
    }
    private void handleFetchedRecords(ConsumerRecords<String, Weather> records) {
        if (records.count() > 0) {
            List<TopicPartition> partitionsToPause = new ArrayList<>();
            records.partitions().forEach(partition -> {
                List<ConsumerRecord<String, Weather>> partitionRecords = records.records(partition);
                ConsumerThreadHandler task = new ConsumerThreadHandler(partitionRecords);
                partitionsToPause.add(partition);
                executor.submit(task);
                activeTasks.put(partition, task);
            });
            consumer.pause(partitionsToPause);
        }
    }

    private void commitOffsets() {
        try {
            long currentTimeMillis = System.currentTimeMillis();
            if (currentTimeMillis - lastCommitTime > 5000) {
                if(!offsetsToCommit.isEmpty()) {
                    consumer.commitSync(offsetsToCommit);
                    offsetsToCommit.clear();
                }
                lastCommitTime = currentTimeMillis;
                log.info("committed");
            }
        } catch (Exception e) {
            log.error("Failed to commit offsets!", e);
        }
    }


    private void checkActiveTasks() {
        List<TopicPartition> finishedTasksPartitions = new ArrayList<>();
        activeTasks.forEach((partition, task) -> {
            if (task.isFinished())
                finishedTasksPartitions.add(partition);
            long offset = task.getCurrentOffset();
            if (offset > 0)
                offsetsToCommit.put(partition, new OffsetAndMetadata(offset));
        });
        finishedTasksPartitions.forEach(partition -> activeTasks.remove(partition));
        consumer.resume(finishedTasksPartitions);
    }


    public Map<String, Object> consumerConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.2.47:29092");
        //props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);    //1000 polls
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 150000);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "weatherSubscriber");
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        //props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
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
}