package com.dayo.weather.entity;

import lombok.Data;
import org.json.JSONException;
import org.json.JSONObject;


@Data
public class Weather {

    private Integer id;
    private String phyQt;
    private Double lon;
    private Double lat;
    private Long timestamp;

    public Weather(int id, String phyQt, double lat,double lon,long zonedDateTime){
        this.id = id;
        this.phyQt=phyQt;
        this.lat=lat;
        this.lon=lon;
        this.timestamp = zonedDateTime;
    }

    public Weather(){}

    public String toString(){
       return "{\"id\":"+this.id+",\"phyQt\":"+this.phyQt+",\"lat\":"+this.lat+",\"lon\":"+this.lon+",\"timestamp\":"+this.timestamp+"}";
    }

    public JSONObject json() throws JSONException {
        JSONObject obj = new JSONObject();
        obj.put("id", this.id);
        obj.put("lat", this.lat);
        obj.put("lon", this.lon);
        obj.put("phyQt",this.phyQt);
        obj.put("timestamp", this.timestamp);
        return obj;
    }
}
