package com.dayo.weather.exception;

import lombok.extern.log4j.Log4j2;
import lombok.extern.slf4j.Slf4j;
import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;

import java.lang.reflect.Method;
@Log4j2
public class AsyncExceptionHandler implements AsyncUncaughtExceptionHandler {
        @Override
        public void handleUncaughtException(Throwable throwable, Method method, Object... obj) {
            StringBuilder parameterBuilder = new StringBuilder("");
            for (Object param : obj) {
                parameterBuilder.append(param);
            }
            log.warn("Exception message - " + throwable.getMessage()
                    + " Method name - " + method.getName()
                    + " Parameters - " + parameterBuilder.toString());
        }
}
