package com.dayo.weather.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import lombok.Data;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

@Data
//@JsonIgnoreProperties(ignoreUnknown = true)
//@JsonRootName("weather")
public class Weather {//implements Serializable {

    public Integer id;
    private String phyQt;
    private Double lon;
    private Double lat;
    private Long zonedDateTime;

    public Weather( int id,String phyQt, double lat,double lon, long zonedDateTime){
        this.id = id;
        this.phyQt=phyQt;
        this.lat=lat;
        this.lon=lon;
        this.zonedDateTime = zonedDateTime;
    }

    public Weather(){}

    public Integer getId() {
        return id;
    }

    public String getPhyQt() {
        return phyQt;
    }

    public Double getLon() {
        return lon;
    }

    public Double getLat() {
        return lat;
    }

    public Long getZonedDateTime(){
        return zonedDateTime;//ZonedDateTime.ofInstant(Instant.ofEpochSecond(zonedDateTime.toEpochSecond()), ZoneId.of("UTC"));
    }

    public String toString(){
        return "{"+"id : " +this.id + ", lat : " +this.lat + ", lon : "+ this.lon+", phyQt : "+this.phyQt+", timestamp: "+ this.zonedDateTime+"}";
    }
    public JSONObject json() throws JSONException {
        JSONObject obj = new JSONObject();

        obj.put("id", this.id);
        obj.put("lat", this.lat);
        obj.put("lon", this.lon);
        obj.put("phyQt",this.phyQt);
        obj.put("timestamp", this.zonedDateTime);
        return obj;
    }
}
