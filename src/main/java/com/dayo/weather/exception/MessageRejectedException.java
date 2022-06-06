package com.dayo.weather.exception;

public class MessageRejectedException extends Exception{
    private StatusCode statusCode;
    private Object[] arguments;

    public MessageRejectedException(StatusCode statusCode, Object... arguments) {
        super(statusCode.getCode() + ": " + String.format(statusCode.getMessage(), arguments));
        this.arguments = arguments;
        this.statusCode = statusCode;
    }
    @Override
    public String getMessage() {
        return String.format(statusCode.getMessage(), arguments);
    }
}
