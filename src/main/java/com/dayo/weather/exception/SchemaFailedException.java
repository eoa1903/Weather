package com.dayo.weather.exception;

public class SchemaFailedException extends Exception{
    private StatusCode statusCode;
    private Object[] arguments;
    public SchemaFailedException(StatusCode statusCode, Object... arguments){
        super(statusCode.getCode() + ": " + String.format(statusCode.getMessage(), arguments));
        this.arguments = arguments;
        this.statusCode = statusCode;
    }

}
