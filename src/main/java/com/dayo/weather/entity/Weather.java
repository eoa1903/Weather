package com.dayo.weather.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.Serializable;


@Data
@JsonIgnoreProperties(ignoreUnknown = true)
//@JsonRootName("weather")
public class Weather implements Serializable {

    public Integer id;
    private String phyQt;
    private Double lon;
    private Double lat;
    public Long zoneDateTime;

    @JsonCreator
    public Weather(@JsonProperty("id")int id, @JsonProperty("phyQt")String phyQt, @JsonProperty("lat")double lat, @JsonProperty("lon")double lon, @JsonProperty("zonedDateTime")long zonedDateTime){
        this.id = id;
        this.phyQt=phyQt;
        this.lat=lat;
        this.lon=lon;
        this.zoneDateTime = zonedDateTime;
       // System.out.println(this.timestamp);
    }

    public Weather(){}


//    public String toString(){
//       return "{\"id\":"+this.id+",\"phyQt\":"+this.phyQt+",\"lat\":"+this.lat+",\"lon\":"+this.lon+",\"timestamp\":"+this.timestamp+"}";
//    }

    public JSONObject json() throws JSONException {
        JSONObject obj = new JSONObject();

        obj.put("id", this.id);
        obj.put("lat", this.lat);
        obj.put("lon", this.lon);
        obj.put("phyQt",this.phyQt);
        obj.put("timestamp", this.zoneDateTime);
        return obj;
    }
}
