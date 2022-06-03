package com.dayo.weather.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.providers.PooledConnectionProvider;

@Configuration
public class RedisConfig {

    @Bean
    public RedissonClient getRedis(){
        try {
            Config config = new Config();
            config.useSingleServer().setAddress("redis://[IP ADDRESS]:6379");
            return Redisson.create(config);
        }
        catch (Exception e){ return null;}
    }

    @Bean
    public UnifiedJedis getJedis(){
        HostAndPort config= new HostAndPort("[IP ADDRESS]", 6379);
        PooledConnectionProvider provider= new PooledConnectionProvider(config);
        UnifiedJedis client= new UnifiedJedis(provider);
        return client;
    }
}
