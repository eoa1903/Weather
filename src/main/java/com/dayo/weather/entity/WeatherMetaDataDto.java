package com.dayo.weather.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.io.Serializable;
import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WeatherMetaDataDto implements Serializable {
    private Instant lasttimestamp;
}
