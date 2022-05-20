package com.dayo.weather.entity;

import lombok.Data;


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
}
