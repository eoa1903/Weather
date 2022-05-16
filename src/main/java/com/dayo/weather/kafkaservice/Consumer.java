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
    private Map<String,Object> map;
    private Iterator<?> set;
    private int weather_id;
    private JsonObject json,redis_fields;
    private Object obj;
    private Iterator<JsonElement> variables;
    private long difference, differenceSecs,differenceMinutes,differenceHours,differenceDays;


   @KafkaListener( topics = "weather-data", concurrency = "1",groupId = "weatherSubscriber")
    public void listener(ConsumerRecords<String, Weather>records) throws JSONException {          //polling maximum of 1000 records every 5sec

       //log.info("Error ->{}",records.count());
        //Go through each batch of records
        //System.out.println("Time received"+ Instant.now(Clock.systemUTC()));
        //log.info("Time received -> {}", Instant.now(Clock.systemUTC()));
        for (ConsumerRecord<String,Weather> m:records) {
            long time=(ZonedDateTime.ofInstant(Instant.now(),zoneId).toInstant().toEpochMilli() - m.timestamp())/ (1000*60) %60;
            if(time <10) {
               // break;
             }
           set = m.value().json().keys();                                                   //id,phyQt,lat,lon,timestamp:5
          map = new HashMap<>();

          //Get All keys and DataTypes of the json obj
          while(set.hasNext()) {
              Object value = set.next();
              map.put(value.toString(), m.value().json().get(value.toString()).getClass().getSimpleName());
          }

//           if(isValid(m,map)) {
//               //m=null;
//               log.info("SUCCESSFUL -> {}, Weather time -> {}, consumer Time -> {}", m.value().json().toString(), m.value().getZonedDateTime(), zonedDateTime);
//
//           }else {
//               log.info("FAILED -> {}, Weather time -> {}, consumer Time -> {}", m.value().json().toString(),m.value().getZonedDateTime(),zonedDateTime );
//               m = null;
//           }//discard data

       }
        //System.out.println("Time done"+ Instant.now(Clock.systemUTC()));
        //log.info("Time Done -> {}, size -> {}", Instant.now(Clock.systemUTC()),records.count());
        //countDownLatch0.countDown();                                                        //Begin countdown
    }

   public boolean isValid(ConsumerRecord<String,Weather> data, Map<String,Object>map ) throws JSONException{
       zonedDateTime =ZonedDateTime.now().withZoneSameInstant(zoneId);
        weather_id = data.value().json().getInt("id");                                    //Weather's ID sent by the user
       obj = client.jsonGet("id_"+weather_id);                                               //we verify with db with a json result or null

       //Step 1. Check if the keys exists
       if(obj.toString() != null){

           json = JsonParser.parseString(obj.toString()).getAsJsonObject();
           policy_type = json.get("policy_time_name").getAsString();
           policy_time = json.get("policy_time_value").getAsLong();
           //System.out.println(data.value().toString());
           timestamp = data.value().getZonedDateTime();//ZonedDateTime.ofInstant( Instant.parse(data.value().json().getStr("timestamp").replace("[UTC]","")) , zoneId );

           difference = zonedDateTime.toInstant().toEpochMilli()-data.value().getZonedDateTime();
           differenceSecs = difference / 1000 % 60;
           differenceMinutes = difference/ (60 * 1000) % 60;
           differenceHours = difference / (60 * 60 * 1000) % 24;
           differenceDays = difference / (24 * 60 * 60 * 1000);
           log.info("differences ->{} Days {} hours {} mins {} secs, policy_type-> {}, policy_value -> {}",differenceDays,differenceHours,differenceMinutes, differenceSecs, policy_type,policy_time);


           //Step 2. Check if the Policy time matches.
           switch(policy_type){
               case "secs":

                   if(differenceMinutes==0&&differenceDays==0 &&differenceHours==0){
                        if(differenceSecs>policy_time)
                            return false;
                    }
                   else{
                        log.info("Failed by secs timeConsumed -> {} timeWeather -> {} differences ->{} Days {} hours {} mins {} secs",zonedDateTime.toInstant().toEpochMilli(),timestamp,differenceDays,differenceHours,differenceMinutes, differenceSecs);
                        return false;
                   }
                   break;

               case "mins":
                   if(differenceDays==0 && differenceHours==0) {
                       if(differenceMinutes <= policy_time ) {
                           if (differenceSecs > 0 && differenceMinutes == policy_time) {
                               return false;
                           }
                       }
                       else
                           return false;
                   }
                   else{
                       log.info("Failed by mins timeConsumed -> {} timeWeather -> {} differences ->{} Days {} hours {} mins {} secs",zonedDateTime.toInstant().toEpochMilli(),timestamp, differenceDays,differenceHours,differenceMinutes, differenceSecs);
                       return false;
                   }
                   break;

               case "hours":
                   if(differenceDays==0) {
                       if(differenceHours <= policy_time){
                           if(differenceMinutes>0|| differenceSecs>0 && differenceHours ==policy_time)
                               return false;
                       }
                       else
                           return false;
                   }
                   else{
                       log.info("Failed by mins timeConsumed -> {} timeWeather -> {} differences ->{} Days {} hours {} mins {} secs",zonedDateTime.toInstant().toEpochMilli(),timestamp, differenceDays,differenceHours,differenceMinutes, differenceSecs);
                       return false;
                   }
                   break;

               case "days":
                   if(differenceDays >= policy_time &&(differenceMinutes >0||differenceHours>0||differenceSecs>0)) {
                       log.info("Failed by days timeConsumed -> {} timeWeather -> {} differences ->{} Days {} hours {} mins {} secs",zonedDateTime.toInstant().toEpochMilli(),timestamp, differenceDays,differenceHours,differenceMinutes, differenceSecs);
                       return false;
                   }
                   break;
           }

           // Step 3. Check the number of  field matches with Redis Schema
           if(map.size() == json.getAsJsonArray("schema").size()){                                                             //to Make sure the number of fields sent is the required num in the db
               variables = json.getAsJsonArray("schema").iterator();                                    //get Iterator for all json element in schema


               // Step 4. Enter a while loop to verify the schemas
               while(variables.hasNext()){                                                                          //while the loop through them
                   redis_fields = variables.next().getAsJsonObject();   //Get the each OBject one after the other

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