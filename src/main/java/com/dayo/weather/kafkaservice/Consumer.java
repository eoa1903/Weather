package com.dayo.weather.kafkaservice;
import com.dayo.weather.entity.Weather;
import com.google.gson.*;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.json.JSONException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.providers.PooledConnectionProvider;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.*;
import java.util.concurrent.CountDownLatch;

@Service
@Log4j2
public class Consumer {
    //public CountDownLatch countDownLatch0 = new CountDownLatch(3);

    private HostAndPort config = new HostAndPort("192.168.2.47", 6379);
    private PooledConnectionProvider provider = new PooledConnectionProvider(config);
    private UnifiedJedis client = new UnifiedJedis(provider);
    private ZoneId zoneId= ZoneId.of("UTC");
    private ZonedDateTime zonedDateTime;
    private long timestamp;
    private String policy_type;
    private long policy_time;
    private long time_holder;
    private Map<String,Object> map;
    private Iterator<?> set;
    private int weather_id;
    private JsonObject json,redis_fields;
    private Object obj;
    private Iterator<JsonElement> variables;
    private long difference, differenceSecs,differenceMinutes,differenceHours,differenceDays;


   @KafkaListener( topics = "weather-data", concurrency = "1",groupId = "weatherSubscriber")
    public void listener(ConsumerRecords<String, Weather>records) throws JSONException {          //polling maximum of 1000 records every 5sec
        for (ConsumerRecord<String,Weather> m:records) {
            long time=(ZonedDateTime.ofInstant(Instant.now(),zoneId).toInstant().toEpochMilli() - m.timestamp())/ (1000*60) %60;
            log.info("Data -> {}",m);
            set = m.value().json().keys();                                                   //id,phyQt,lat,lon,timestamp:5
            map = new HashMap<>();

          //Get All keys and DataTypes of the json obj
          while(set.hasNext()) {
              Object value = set.next();
              map.put(value.toString(), m.value().json().get(value.toString()).getClass().getSimpleName());
          }

           if(isValid(m,map)) {
               log.info("SUCCESSFUL -> {}, Weather time -> {}, consumer Time -> {}", m.value().json().toString(), m.value().getZonedDateTime(), zonedDateTime);

           }else {
               log.info("FAILED -> {}, Weather time -> {}, consumer Time -> {}", m.value().json().toString(),m.value().getZonedDateTime(),zonedDateTime );
               m = null;
           }

       }
    }

   public boolean isValid(ConsumerRecord<String,Weather> data, Map<String,Object>map ) throws JSONException{
       zonedDateTime =ZonedDateTime.now().withZoneSameInstant(zoneId);
       // weather_id = data.;                                      //Weather's ID sent by the user
       obj = client.jsonGet("id_"+data.key());                                               //we verify with db with a json result or null

       //Step 1. Check if the keys exists
       if(obj.toString() != null){

           json = JsonParser.parseString(obj.toString()).getAsJsonObject();
           policy_type = json.get("policy_time_name").getAsString();
           policy_time = json.get("policy_time_value").getAsLong();
           timestamp = data.value().json().getLong("timestamp");
           difference = zonedDateTime.toInstant().toEpochMilli() - data.value().getZonedDateTime();
           differenceSecs = difference / 1000 % 60;
           differenceMinutes = difference/ (60 * 1000) % 60;
           differenceHours = difference / (60 * 60 * 1000) % 24;
           differenceDays = difference / (24 * 60 * 60 * 1000);
           log.info("differences ->{} Days {} hours {} mins {} secs, policy_type-> {}, policy_value -> {}",differenceDays,differenceHours,differenceMinutes, differenceSecs, policy_type,policy_time);


           //Step 2. Check if the Policy time matches.
           switch(policy_type){
               case "secs":
                   if(time_holder != 0){
                       if((timestamp - time_holder) >= (policy_time*1000)){
                           time_holder = timestamp;
                       }
                       else{
                           return false;
                       }
                   }
                   else{
                       time_holder = timestamp;
                   }
                   break;
               case "days":
                   if(time_holder != 0){
                       if((timestamp - time_holder) >= (policy_time*24 * 60 * 60 * 1000)){
                           time_holder = timestamp;
                       }
                       else{
                           return false;
                       }
                   }
                   else{
                       time_holder = timestamp;
                   }
                   break;
               case "mins":
                   if(time_holder != 0){
                       if((timestamp - time_holder) >= (policy_time*60 * 1000) ){
                           time_holder = timestamp;
                       }
                       else{
                           return false;
                       }
                   }
                   else{
                       time_holder = timestamp;
                   }
                   break;
               case "hours":
                   if(time_holder != 0){
                       if((timestamp - time_holder) >= (policy_time*60 * 60 * 1000)){
                           time_holder = timestamp;
                       }
                       else{
                           return false;
                       }
                   }
                   else{
                       time_holder = timestamp;
                   }
                   break;
           }

           // Step 3. Check the number of  field matches with Redis Schema
           if(map.size() == json.getAsJsonArray("schema").size()){                                                             //to Make sure the number of fields sent is the required num in the db
               variables = json.getAsJsonArray("schema").iterator();                                    //get Iterator for all json element in schema


               // Step 4. Enter a while loop to verify the schemas
               while(variables.hasNext()){                                                                          //while the loop through them
                   redis_fields = variables.next().getAsJsonObject();               //Get the each OBject one after the other

                   if(map.containsKey(redis_fields.get("name").getAsString())
                           && !map.get(redis_fields.get("name").getAsString())
                           .equals(redis_fields.get("type").getAsString())) {              // if the field and name doesn't exist we out
                       log.info("Failed: redis key -> {} ,  weather key -> {}, type Redis -> {} , weather type -> {}",redis_fields.get("name").getAsString(),map.containsKey(redis_fields.get("name").getAsString()),redis_fields.get("type").getAsString(),map.get(redis_fields.get("name").getAsString()) );
                       return false;
                   }
               }
           }
           else
               return false;                                                                                                    //if the size isn't equal to num of required params in the database return false
       }
       else
           return false;                                                                                                        //if not found
       return true;
   }
}


/**
 Map<String,Object> y = data.value().json().toMap();
 if(data.value()
 .json().toMap().containsKey(redis_fields.get("name").getAsString())&&
 !data.value()
 .json().toMap().get(redis_fields.get("name").getAsString()).getClass().getSimpleName()
 .equals(redis_fields.get("type").getAsString())){
 */