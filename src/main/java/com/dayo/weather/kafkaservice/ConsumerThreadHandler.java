package com.dayo.weather.kafkaservice;

import com.dayo.weather.entity.Weather;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.json.JSONException;
import org.json.JSONObject;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.providers.PooledConnectionProvider;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Log4j2
public class ConsumerThreadHandler implements Runnable{
    private List<ConsumerRecord<String, Weather> >consumerRecords;
    private ConsumerRecord<String, Weather> consumerRecord;
    private HostAndPort config = new HostAndPort("192.168.2.47", 6379);
    private PooledConnectionProvider provider = new PooledConnectionProvider(config);
    private UnifiedJedis client = new UnifiedJedis(provider);


    private static Map<String,Long> time_holder = new HashMap<>();
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();
    private volatile boolean finished = false;
    private final CompletableFuture<Long> completion = new CompletableFuture<>();
    private static int counter;

    private long timestamp;
    private String policy_type;
    private long policy_time;
    private String feed_id;
    private JsonObject json,redis_fields;
    private Object obj;
    private Iterator<JsonElement> variables;
    private final AtomicLong currentOffset = new AtomicLong();
    private volatile boolean stopped = false;

    private volatile boolean started = false;

    public ConsumerThreadHandler(List<ConsumerRecord<String,Weather>> consumerRecords) {
        this.consumerRecords = consumerRecords;
    }

    @Override
    public void run() {
        if (stopped){
            return;
        }
        //log.info("SIze {}, thread {}",consumerRecord1.size(), Thread.currentThread().getId());
        for (ConsumerRecord<String,Weather> consumerRecord:consumerRecords) {
            if (stopped)
                break;
            this.consumerRecord=consumerRecord;
            currentOffset.set(consumerRecord.offset());

            //log.info("Offsets -> {}, By Thread {}", m.offset(), Thread.currentThread().getId());


            if(isPolicyTimeValid() && isSchemaValid()){
                log.info("SUCCESSFUL -> key {} previous timestamp {}, Weather time -> {}", consumerRecord.key(), time_holder.get("id_"+consumerRecord.key()), consumerRecord.value().getTimestamp());
            }
            else {
                //log.info("Not valid Data key {} previous timestamp {}, Weather time -> {}", consumerRecord.key(), time_holder.get("id_"+consumerRecord.key()), consumerRecord.value().getTimestamp());
            }
            finished = true;
            completion.complete(currentOffset.get());
        }
        //finished = true;
        //completion.complete(currentOffset.get());
        log.info("counter -> {}", counter);
    }

    /**
     * Policy Validator
     * @return true or false
     */
    public boolean isPolicyTimeValid(){
        try{
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
                    if(getTime(feed_id)!=0){
                        if(timestamp-getTime(feed_id) >(policy_time * 1000) || (timestamp -getTime(feed_id)) <0)
                            updateTime(feed_id,timestamp);
                        else {
                            log.info("Failed -> {}, feed_id {}, difference {}", timestamp, feed_id, timestamp - time_holder.get(feed_id));
                            return false;
                        }
                    }
                    else{
                        updateTime(feed_id,timestamp);
                        log.info("Succ -> {}, feed_id {}", timestamp, feed_id);
                    }
                    break;

                case "mins":
                    if(getTime(feed_id) !=0){
                        if(timestamp-getTime(feed_id) >(policy_time * 1000*60) || (timestamp -getTime(feed_id)) <0)
                            updateTime(feed_id,timestamp);
                        else {
                            return false;
                        }
                    }
                    else{
                        updateTime(feed_id,timestamp);
                    }
                    break;

                case "hours":
                    if(getTime(feed_id) !=0){
                        if(timestamp-getTime(feed_id) >(policy_time * 1000*60*60) || (timestamp -getTime(feed_id)) <0)
                            updateTime(feed_id,timestamp);
                        else {
                            return false;
                        }
                    }
                    else{
                        updateTime(feed_id,timestamp);
                    }
                    break;

                case "days":
                    if(getTime(feed_id) !=0){
                        if(timestamp-getTime(feed_id) >(policy_time * 1000*60*60*24) || (timestamp -getTime(feed_id)) <0)
                            updateTime(feed_id,timestamp);
                        else {
                            return false;
                        }
                    }
                    else{
                        updateTime(feed_id,timestamp);
                    }
                    break;
            }
        }
        catch (NullPointerException e){return false;}
        return true;
    }

    /**
     * Schema Validator
     * @return true or false
     */
    public boolean isSchemaValid(){
        try{
            variables = json.getAsJsonArray("schema").iterator();
            while (variables.hasNext()) {
                redis_fields = variables.next().getAsJsonObject();
                if (!doesObjectContainField(redis_fields.get("name").getAsString())) {
                    return false;
                }
            }
        }catch (Exception e){

        }
        return true;
    }

    public boolean doesObjectContainField(String fieldName) {
        return Arrays.stream(consumerRecord.value().getClass().getDeclaredFields())
                .anyMatch(f -> f.getName().equals(fieldName));
    }

    /**
     *
     * @param key
     * @param timestamp
     */
    public void updateTime(String key, long timestamp){
        writeLock.lock();
        try{
            time_holder.put(key, timestamp);
        }
        finally{
            writeLock.unlock();
        }
    }

    /**
     *
     * @param key Feed Id
     * @return timestamp
     */
    public long getTime(String key){
        readLock.lock();
        try{
            return time_holder.get(key) == null?0:time_holder.get(key);
        }
        finally {
            readLock.unlock();
        }
    }

    public boolean isFinished() {
        return finished;
    }
    public long getCurrentOffset() {
        return currentOffset.get();
    }
}
