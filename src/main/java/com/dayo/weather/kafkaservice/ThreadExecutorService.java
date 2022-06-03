package com.dayo.weather.kafkaservice;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@EnableAsync(proxyTargetClass = true)
@Configuration
public class ThreadExecutorService implements AsyncConfigurer {
    @Override
    @Bean(name = "threadExecutor")
    public ThreadPoolTaskExecutor getAsyncExecutor(){
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(20);
        executor.setMaxPoolSize(200);
        executor.setQueueCapacity(0);
        executor.setAllowCoreThreadTimeOut(true); //allow threads that are idle to release their memory
        executor.initialize();
        return executor;
    }

}
