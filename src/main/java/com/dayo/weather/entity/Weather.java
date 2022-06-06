package com.dayo.weather.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;


@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Weather implements Serializable {

    private Integer id;
    private String phyQt;
    private Double lon;
    private Double lat;
    private Long timestamp;

    public String toString(){
       return "{\"id\":"+this.id+",\"phyQt\":"+this.phyQt+",\"lat\":"+this.lat+",\"lon\":"+this.lon+",\"timestamp\":"+this.timestamp+"}";
    }
}
