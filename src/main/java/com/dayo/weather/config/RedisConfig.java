package com.dayo.weather.config;

import lombok.extern.log4j.Log4j2;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.providers.ConnectionProvider;
import redis.clients.jedis.providers.PooledConnectionProvider;
import redis.clients.jedis.providers.ShardedConnectionProvider;

import java.util.Arrays;
import java.util.List;

@Configuration
@Log4j2
public class RedisConfig {

    @Bean
    public RedissonClient getRedis(){
        Config config = new Config();
        config.setNettyThreads(2);
        try {
            config.useSingleServer()
                    .setAddress("redis://192.168.2.47:6379")
                    .setConnectionMinimumIdleSize(4)
                    .setRetryAttempts(5)
                    .setRetryInterval(10);

        }
        catch (Exception e){
            log.warn("Redis Connection failed: {}",e.getMessage());
        }
        return Redisson.create(config);
    }

    @Bean
    public UnifiedJedis getJedis(){
        List<HostAndPort> config= Arrays.asList(new HostAndPort("192.168.2.47", 6379));
        ShardedConnectionProvider provider= new ShardedConnectionProvider(config);
        UnifiedJedis client= new UnifiedJedis(provider);
        return client;
    }
}
