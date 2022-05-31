package com.dayo.weather.kafkaservice;

import com.dayo.weather.entity.Weather;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.providers.PooledConnectionProvider;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Log4j2
public class ConsumerThreadHandler implements Runnable {
    private ConsumerRecords<String, Weather> consumerRecords;
    private ConsumerRecord<String, Weather> consumerRecord;

    private static HostAndPort config = new HostAndPort("192.168.2.47", 6379);
    private static PooledConnectionProvider provider = new PooledConnectionProvider(config);
    private static UnifiedJedis client = new UnifiedJedis(provider);

    private static Map<String, Long> time_holder = new ConcurrentHashMap<>();
    private long timestamp;
    private String policy_type;
    private long policy_time;
    private String feed_id;
    private JsonObject json, redis_fields;
    private Object obj;
    private Iterator<JsonElement> variables;


    public ConsumerThreadHandler(ConsumerRecords<String, Weather> consumerRecords) {
        this.consumerRecords = consumerRecords;
    }

    public ConsumerThreadHandler(ConsumerRecord<String, Weather> consumerRecord) {
        this.consumerRecord = consumerRecord;
    }

    @Override
    public void run() {
        for (ConsumerRecord<String, Weather> consumerRecord : consumerRecords) {
            this.consumerRecord = consumerRecord;

            if (isPolicyTimeValid() && isSchemaValid()) {
                log.info("SUCCESSFUL");
            } else {
                log.info("Not valid");
            }
        }
    }

    /**
     * Policy Time Validator
     *
     * @return true or false
     */
    public boolean isPolicyTimeValid() {
        try {
            //Get Redis Json
            feed_id = "id_" + consumerRecord.key();
            obj = client.jsonGet(feed_id);
            json = JsonParser.parseString(obj.toString()).getAsJsonObject();

            //Get Policy time, policy name
            policy_type = json.get("policy_time_name").getAsString();
            policy_time = json.get("policy_time_value").getAsLong();
            timestamp = consumerRecord.value().getTimestamp();

            switch (policy_type) {
                case "secs":
                    return isSecsValid(feed_id);
                case "days":
                    return isDaysValid(feed_id);
                case "mins":
                    return isMinsValid(feed_id);
                case "hours":
                    return isHoursValid(feed_id);
            }
            return true;
        } catch (NullPointerException e) {
            return false;
        }

    }

    public boolean isSecsValid(String feed_id){
        if (time_holder.containsKey(feed_id)) {
            if ((timestamp - time_holder.get(feed_id)) > (policy_time * 1000) || (timestamp - time_holder.get(feed_id)) < 0) {
                log.info("Updated -> {}, previous {} feed_id {},difference {}", timestamp, time_holder.get(feed_id), feed_id, timestamp - time_holder.get(feed_id));
                time_holder.put(feed_id, timestamp);
            } else {
                log.info(" failed -> {}, previous {} feed_id {}, difference {}", timestamp, time_holder.get(feed_id), feed_id, timestamp - time_holder.get(feed_id));
                return false;
            }
        } else {
            time_holder.put(feed_id, timestamp);
            log.info("Succ -> {}, feed_id {}", timestamp, feed_id);
        }
        return true;
    }

    public boolean isDaysValid(String feed_id){
        if (time_holder.containsKey(feed_id)) {
            if ((timestamp - time_holder.get(feed_id)) > (policy_time * 1000*24*60*60) || (timestamp - time_holder.get(feed_id)) < 0) {
                log.info("Updated -> {}, previous {} feed_id {},difference {}", timestamp, time_holder.get(feed_id), feed_id, timestamp - time_holder.get(feed_id));
                time_holder.put(feed_id, timestamp);
            } else {
                log.info(" failed -> {}, previous {} feed_id {}, difference {}", timestamp, time_holder.get(feed_id), feed_id, timestamp - time_holder.get(feed_id));
                return false;
            }
        } else {
            time_holder.put(feed_id, timestamp);
            log.info("Succ -> {}, feed_id {}", timestamp, feed_id);
        }
        return true;
    }

    public boolean isMinsValid(String feed_id){
        if (time_holder.containsKey(feed_id)) {
            if ((timestamp - time_holder.get(feed_id)) > (policy_time * 1000*60) || (timestamp - time_holder.get(feed_id)) < 0) {
                log.info("Updated -> {}, previous {} feed_id {},difference {}", timestamp, time_holder.get(feed_id), feed_id, timestamp - time_holder.get(feed_id));
                time_holder.put(feed_id, timestamp);
            } else {
                log.info(" failed -> {}, previous {} feed_id {}, difference {}", timestamp, time_holder.get(feed_id), feed_id, timestamp - time_holder.get(feed_id));
                return false;
            }
        } else {
            time_holder.put(feed_id, timestamp);
            log.info("Succ -> {}, feed_id {}", timestamp, feed_id);
        }
        return true;
    }

    public boolean isHoursValid(String feed_id){
        if (time_holder.containsKey(feed_id)) {
            if ((timestamp - time_holder.get(feed_id)) > (policy_time * 1000*60*60) || (timestamp - time_holder.get(feed_id)) < 0) {
                log.info("Updated -> {}, previous {} feed_id {},difference {}", timestamp, time_holder.get(feed_id), feed_id, timestamp - time_holder.get(feed_id));
                time_holder.put(feed_id, timestamp);
            } else {
                log.info(" failed -> {}, previous {} feed_id {}, difference {}", timestamp, time_holder.get(feed_id), feed_id, timestamp - time_holder.get(feed_id));
                return false;
            }
        } else {
            time_holder.put(feed_id, timestamp);
            log.info("Succ -> {}, feed_id {}", timestamp, feed_id);
        }
        return true;
    }

    /**
     * Schema Validator
     *
     * @return true or false
     */
    public boolean isSchemaValid() {
        try {
            variables = json.getAsJsonArray("schema").iterator();
            while (variables.hasNext()) {
                redis_fields = variables.next().getAsJsonObject();
                if (!doesObjectContainField(redis_fields.get("name").getAsString())) {
                    return false;
                }
            }
        } catch (Exception e) {}
        return true;
    }

    public boolean doesObjectContainField(String fieldName) {
        return Arrays.stream(consumerRecord.value().getClass().getDeclaredFields())
                .anyMatch(f -> f.getName().equals(fieldName));
    }
}
