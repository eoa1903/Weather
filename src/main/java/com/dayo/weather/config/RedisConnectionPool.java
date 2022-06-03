package com.dayo.weather.config;

import com.dayo.weather.entity.WeatherMetaDataDto;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import redis.clients.jedis.UnifiedJedis;
import java.util.Iterator;
import java.util.Optional;

@Component
@Log4j2
@SuppressWarnings("FieldCanBeLocal")
public class RedisConnectionPool {
    @Autowired
    public RedissonClient redissonClient;
    @Autowired
    public UnifiedJedis jedis;


    private static RBucket<String> bucket;

    private static final ObjectMapper objectMapper=new ObjectMapper();

    static {
        objectMapper.registerModule(new JavaTimeModule());
    }

    public JsonObject getFeed(String feedID){
        Object obj;
        try{
            obj = jedis.jsonGet(feedID);
            return JsonParser.parseString(obj.toString()).getAsJsonObject();
        }
        catch (Exception e){ return  null;}
    }

    public Iterator<JsonElement> getSchema(String feedID){
        JsonObject json = getFeed(feedID);
        return json.getAsJsonArray("schema").iterator();

    }
    public Long getPolicyTime(String feedID){
        return getFeed(feedID).get("policy_time_value").getAsLong();
    }
    public String getPolicyTimeName(String feedID){
        return getFeed(feedID).get("policy_time_name").getAsString();
    }
    public WeatherMetaDataDto getWeatherMetaDataDto(String feedID){
        return Optional.ofNullable(getWeatherMetadataDtoBucket(feedID).get())
                .map(this::readMetadataDtoFromJson)
                .orElse(null);
    }
    public void updateRefreshTimeStamp(String feedID, WeatherMetaDataDto data) throws JsonProcessingException {
        bucket = redissonClient.getBucket("weatherMetadata_"+feedID);
        bucket.set(objectMapper.writeValueAsString(data));
    }

    public RBucket<String> getWeatherMetadataDtoBucket(String feedID){
        return redissonClient.getBucket("weatherMetadata_"+feedID);
    }

    @SneakyThrows
    private WeatherMetaDataDto readMetadataDtoFromJson(String json) {
        return objectMapper.readValue(json, WeatherMetaDataDto.class);
    }
}
