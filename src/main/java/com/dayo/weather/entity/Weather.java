package com.dayo.weather.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;


@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Weather implements Serializable {

    @JsonProperty("id")
    private Integer id;
    @JsonProperty("phyQt")
    private String phyQt;
    @JsonProperty("lon")
    private Double lon;
    @JsonProperty("lat")
    private Double lat;
    @JsonProperty("timestamp")
    private Long timestamp;

    public String toString(){
       return "{\"id\":"+this.id+",\"phyQt\":"+this.phyQt+",\"lat\":"+this.lat+",\"lon\":"+this.lon+",\"timestamp\":"+this.timestamp+"}";
    }
}
