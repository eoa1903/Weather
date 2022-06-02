package com.dayo.weather.process;

import com.dayo.weather.config.RedisConnectionPool;
import com.dayo.weather.entity.Weather;
import com.dayo.weather.entity.WeatherMetaDataDto;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import lombok.Synchronized;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.providers.PooledConnectionProvider;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Log4j2
public class MessageProcessor {
    private static Map<String, Long> time_holder = new ConcurrentHashMap<>();
    private long timestamp;
    private String policy_type;
    private long policy_time;
    private String feed_id;
    //private JsonObject json, redis_fields;
    private Object obj;

    @Autowired
    RedisConnectionPool connectionPool;

    public void process(Weather data, String feedID){
        synchronized (this) {
            WeatherMetaDataDto weatherMetaDataDto = connectionPool.getWeatherMetaDataDto(feedID);

            log.info("weathMeta{}", weatherMetaDataDto.toString());
            if (!isMessageTimeValid(data.getTimestamp(), feedID, weatherMetaDataDto)) {
                log.info("Message time is not valid");
            } else {
                log.info("Valid");
            }
            if (!isSchemaValid(data, feedID)) {
                log.info("Message failed schema validation");
            }
        }
    }

    public boolean isMessageTimeValid(Long recordTimestamp,String feedID,WeatherMetaDataDto weatherMetaDataDto){
        Instant lastRefreshTimestamp = Optional.ofNullable(weatherMetaDataDto).map(WeatherMetaDataDto::getLasttimestamp).orElse(null);
        log.info("RefreshTime{}", lastRefreshTimestamp);
        Instant weatherTimeInstant = Instant.ofEpochMilli(recordTimestamp);
        if (weatherTimeInstant.isBefore(lastRefreshTimestamp)) {
            return true;
        }
        Instant offsetTime;
        Long refreshRate = connectionPool.getPolicyTime(feedID);
        String refreshPeriod = connectionPool.getPolicyTimeName(feedID);
        offsetTime = lastRefreshTimestamp.plus(Duration.of(refreshRate, getTemporalUnit(refreshPeriod)));
        //log.info("Record time {}, offsetTime{}", weatherTimeInstant,offsetTime);

        return weatherTimeInstant.isAfter(offsetTime);
    }

    public TemporalUnit getTemporalUnit(String val){
        switch (val) {
            case "secs":
                return ChronoUnit.SECONDS;
            case "mins":
                return ChronoUnit.MINUTES;
            case "hours":
                return ChronoUnit.HOURS;
            case "days":
                return ChronoUnit.DAYS;
            case "weeks":
                return ChronoUnit.WEEKS;
            case "months":
                return ChronoUnit.MONTHS;
            case "millis":
                return ChronoUnit.MILLIS;
            default:
                return ChronoUnit.YEARS;
        }
    }

    public boolean isSchemaValid(Weather data, String feedID) {
        Iterator<JsonElement> variables=null;

        //log.info("here");
        try {
            variables = connectionPool.getSchema(feedID);
            JsonObject redis_fields;
            while (variables.hasNext()) {
                redis_fields = variables.next().getAsJsonObject();
                //log.info("dat {}",redis_fields);
                if (!doesObjectContainField(redis_fields.get("name").getAsString(),data)) {
                    return false;
                }
            }
        } catch (Exception e) {}
        return true;
    }

    public boolean doesObjectContainField(String fieldName,Weather data) {
        return Arrays.stream(data.getClass().getDeclaredFields())
                .anyMatch(f -> f.getName().equals(fieldName));
    }
}
