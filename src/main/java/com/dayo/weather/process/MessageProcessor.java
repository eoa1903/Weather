package com.dayo.weather.process;

import com.dayo.weather.config.RedisConnectionPool;
import com.dayo.weather.entity.Weather;
import com.dayo.weather.entity.WeatherMetaDataDto;
import com.dayo.weather.exception.MessageRejectedException;
import com.dayo.weather.exception.SchemaFailedException;
import com.dayo.weather.exception.StatusCode;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;

@Component
@Log4j2
public class MessageProcessor {
    @Autowired
    RedisConnectionPool connectionPool;

    public synchronized void process(Weather data, String feedID){
        try {
            WeatherMetaDataDto weatherMetaDataDto = connectionPool.getWeatherMetaDataDto(feedID);
            checkMessageWithinLimit(data.getTimestamp(), feedID, weatherMetaDataDto);
            checkifSchemaValid(data, feedID);
            printToKafka();
        }
        catch (Exception e){
            log.warn("Not Valid: {}",e.getMessage());
        }
    }
    public void printToKafka(){}

    public void checkifSchemaValid(Weather data, String feedID) throws SchemaFailedException {
        if (!isSchemaValid(data, feedID)) {
            throw new SchemaFailedException(StatusCode.DATA_DOES_NOT_MATCH_SCHEMA);
        }
    }

    public void checkMessageWithinLimit(Long recordTimestamp,String feedID,WeatherMetaDataDto weatherMetaDataDto) throws JsonProcessingException, MessageRejectedException {
        if (!isMessageTimeValid(recordTimestamp, feedID, weatherMetaDataDto)) {
            throw new MessageRejectedException(StatusCode.FAILED_TIME_VALIDATION,Instant.ofEpochMilli(recordTimestamp), weatherMetaDataDto.getLasttimestamp(),connectionPool.getPolicyTime(feedID),connectionPool.getPolicyTimeName(feedID));
        }
        else {
            log.info("Msg Accepted timestamp{}",Instant.ofEpochMilli(recordTimestamp));
            connectionPool.updateRefreshTimeStamp(feedID, new WeatherMetaDataDto(Instant.ofEpochMilli(recordTimestamp)));
        }
    }

    public boolean isMessageTimeValid(Long recordTimestamp,String feedID,WeatherMetaDataDto weatherMetaDataDto){
        Instant lastRefreshTimestamp = Optional.ofNullable(weatherMetaDataDto).map(WeatherMetaDataDto::getLasttimestamp).orElse(null);
        //log.info("RefreshTime -> {}", lastRefreshTimestamp);

        if(lastRefreshTimestamp == null) {
            log.info("valid data {}, instant {}", recordTimestamp, Instant.ofEpochMilli(recordTimestamp));
            return true;
        }

        Instant weatherTimeInstant = Instant.ofEpochMilli(recordTimestamp);
        if (weatherTimeInstant.isBefore(lastRefreshTimestamp)) {
            return true;
        }
        Instant offsetTime;
        Long refreshRate = connectionPool.getPolicyTime(feedID);
        String refreshPeriod = connectionPool.getPolicyTimeName(feedID);
        offsetTime = lastRefreshTimestamp.plus(Duration.of(refreshRate, getTemporalUnit(refreshPeriod)));

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
