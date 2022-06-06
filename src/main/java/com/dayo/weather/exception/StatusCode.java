package com.dayo.weather.exception;

public enum StatusCode {
    OK(1,"Message is Valid"),
    DATA_DOES_NOT_MATCH_SCHEMA(2, "The feed data does not match the schema."),
    FAILED_TIME_VALIDATION(3,"Message received time: %s. Last refresh time: %s. Message Policy: %s. Message Frequency: %s.");


    private final Integer code;
    private final String message;

    StatusCode(int i, String s) {
        this.code = i;
        this.message = s;
    }
    public Integer getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }
}
