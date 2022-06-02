package com.dayo.weather.config;

import com.dayo.weather.entity.Weather;
import com.dayo.weather.entity.WeatherMetaDataDto;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.providers.PooledConnectionProvider;

import java.util.Iterator;
import java.util.Optional;

@Component
@Log4j2
public class RedisConnectionPool {
    @Autowired
    ObjectMapper objectMapper;
    //@Autowired
    private HostAndPort config= new HostAndPort("192.168.2.47", 6379);
    //@Autowired
    private PooledConnectionProvider provider= new PooledConnectionProvider(config);
    //@Autowired
    private UnifiedJedis client= new UnifiedJedis(provider);
    Config config1 = new Config();
    RedissonClient client1;

    public RedisConnectionPool(){
        config1.useSingleServer()
                .setAddress("redis://192.168.2.47:6379");
        this.client1 = Redisson.create(config1);
    }

//    public RedisConnectionPool(){
//        this.config = new HostAndPort("192.168.2.47", 6379);
//        this.provider = new PooledConnectionProvider(config);
//        this.client= new UnifiedJedis(provider);
//    }

    public JsonObject getFeed(String feedID){
        Object obj;
        try{
            obj = this.client.jsonGet(feedID);
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
    public void setRefreshTimeStamp(String feedID, WeatherMetaDataDto data){
        RBucket<WeatherMetaDataDto> bucket = client1.getBucket(feedID);
        bucket.set(data);
    }

    public RBucket<String> getWeatherMetadataDtoBucket(String feedID){
        return client1.getBucket(feedID);
    }

    @SneakyThrows
    private WeatherMetaDataDto readMetadataDtoFromJson(String json) {
        return objectMapper.readValue(json, WeatherMetaDataDto.class);
    }
}
