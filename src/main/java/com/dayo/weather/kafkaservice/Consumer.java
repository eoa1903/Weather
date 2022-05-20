package com.dayo.weather.kafkaservice;
import com.dayo.weather.entity.Weather;
import com.fasterxml.jackson.core.JsonProcessingException;
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
import java.util.*;

@Service
@Log4j2
public class Consumer {
    //public CountDownLatch countDownLatch0 = new CountDownLatch(3);

    private HostAndPort config = new HostAndPort("192.168.2.47", 6379);
    private PooledConnectionProvider provider = new PooledConnectionProvider(config);
    private UnifiedJedis client = new UnifiedJedis(provider);
    private long timestamp;
    private String policy_type;
    private Object value;
    private long policy_time;
    private Map<String,Object> map;
    private Map<String,Long> time_holder_sec = new HashMap<>();
    private Map<String,Long> time_holder_min = new HashMap<>();
    private Map<String,Long> time_holder_hour = new HashMap<>();
    private Map<String,Long> time_holder_day = new HashMap<>();
    private Iterator<?> set;
    private String feed_id;
    private JsonObject json,redis_fields;
    private Object obj;
    private Iterator<JsonElement> variables;

   @KafkaListener( topics = "weather-data", concurrency = "1",groupId = "weatherSubscriber")
    public void listener(ConsumerRecords<String, Weather>records) throws JSONException {                                //polling maximum of 1000 records every 5sec
        for (ConsumerRecord<String,Weather> m:records) {
            set = m.value().json().keys();                                                                              //JsonParser.parseString(m.value().toString()).getAsJsonObject().keySet().iterator();                                //id,phyQt,lat,lon,timestamp:5
            map = new HashMap<>();
            //log.info("type ->{}, timetsamp {}, value {}",m.timestampType().name,m.timestamp(), m.value().getZonedDateTime());
            //log.info("data -> {} json -> {}", m.value(),m.value().json().toString());
            //log.info("Json ->{}", JsonParser.parseString(m.value().toString()).getAsJsonObject());

          //Get All keys and DataTypes of the json obj
          while(set.hasNext()) {
              value = set.next();
              map.put(value.toString(), m.value().json().get(value.toString()).getClass().getSimpleName());
          }

//           if(isValid(m,map)) {
//               log.info("SUCCESSFUL -> key {} previous timestamp {}, Weather time -> {}", m.key(), time_holder_sec.get("id_"+m.key()), m.value().getZonedDateTime());
//
//           }else {
//               log.info("FAILED -> key {} previous time stamp{}, Weather time -> {}", m.key(),time_holder_sec.get("id_"+m.key()),m.value().getZonedDateTime());
//               m = null;
////               System.gc();
////               System.runFinalization();
//           }
       }
    }

   public boolean isValid(ConsumerRecord<String,Weather> data, Map<String,Object>map ){

       try {
           feed_id = "id_" + data.key();
           obj = client.jsonGet(feed_id);

           //Step 1. Check if the keys exists
           if (obj.toString() != null) {

               json = JsonParser.parseString(obj.toString()).getAsJsonObject();
               policy_type = json.get("policy_time_name").getAsString();
               policy_time = json.get("policy_time_value").getAsLong();
               timestamp = data.value().json().getLong("timestamp");

               //Step 2. Check if the Policy time matches.
               switch (policy_type) {
                   case "secs":
                       if (time_holder_sec.containsKey(feed_id)) {
                           if ((timestamp - time_holder_sec.get(feed_id)) >= (policy_time * 1000) ||(timestamp - time_holder_sec.get(feed_id)) <=0 ) {
                               time_holder_sec.put(feed_id, timestamp);
                           }
                           else {
                               log.info(" -> {}, feed_id {}, difference {}", timestamp, feed_id, timestamp - time_holder_sec.get(feed_id));
                               return false;
                           }
                       } else {
                           time_holder_sec.put(feed_id, timestamp);
                           log.info("Succ -> {}, feed_id {}", timestamp, feed_id);
                       }

                       break;
                   case "days":
                       if (time_holder_day.containsKey(feed_id)) {
                           if ((timestamp - time_holder_day.get(feed_id)) >= (policy_time * 1000 * 24 * 60 * 60) || (timestamp - time_holder_day.get(feed_id)) <=0)
                               time_holder_day.put(feed_id, timestamp);
                           else
                               return false;
                       } else {
                           time_holder_day.put(feed_id, timestamp);
                       }
                       break;
                   case "mins":
                       if (time_holder_min.containsKey(feed_id)) {
                           if ((timestamp - time_holder_min.get(feed_id)) >= (policy_time * 1000 * 60) || (timestamp - time_holder_min.get(feed_id)) <=0)
                               time_holder_min.put(feed_id, timestamp);
                           else
                               return false;
                       } else {
                           time_holder_min.put(feed_id, timestamp);
                       }
                       break;
                   case "hours":
                       if (time_holder_hour.containsKey(feed_id)) {
                           if ((timestamp - time_holder_hour.get(feed_id)) >= (policy_time * 1000 * 24 * 60 * 60) || (timestamp - time_holder_hour.get(feed_id))<=0)
                               time_holder_hour.put(feed_id, timestamp);
                           else
                               return false;
                       } else {
                           time_holder_hour.put(feed_id, timestamp);
                       }
                       break;
               }

               // Step 3. Check the number of  field matches with Redis Schema
               if (map.size() == json.getAsJsonArray("schema").size()) {
                   variables = json.getAsJsonArray("schema").iterator();


                   // Step 4. Enter a while loop to verify the schemas
                   while (variables.hasNext()) {
                       redis_fields = variables.next().getAsJsonObject();

                       if (map.containsKey(redis_fields.get("name").getAsString())
                               && !map.get(redis_fields.get("name").getAsString())
                               .equals(redis_fields.get("type").getAsString())) {
                           //log.info("Failed: redis key -> {} ,  weather key -> {}, type Redis -> {} , weather type -> {}", redis_fields.get("name").getAsString(), map.containsKey(redis_fields.get("name").getAsString()), redis_fields.get("type").getAsString(), map.get(redis_fields.get("name").getAsString()));
                           return false;
                       }
                   }
               } else
                   return false;
           } else
               return false;
       }
       catch (JSONException e){ System.err.println("Json Error "+ e.getMessage());}
       return true;
   }
}