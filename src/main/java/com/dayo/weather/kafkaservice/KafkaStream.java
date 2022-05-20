package com.dayo.weather.kafkaservice;

import com.dayo.weather.entity.Weather;
import com.dayo.weather.serializers.CustomDeserializer;
import com.dayo.weather.serializers.CustomSerializer;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.extern.log4j.Log4j2;
import netscape.javascript.JSObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JsonSerializer;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.providers.PooledConnectionProvider;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.kafka.streams.StreamsConfig.*;

//@Configuration
//@EnableKafka
//@EnableKafkaStreams
@Log4j2
public class KafkaStream {
    private Iterator<String> set;
    private Map<String,Object> map = new HashMap<>();
    private HostAndPort config = new HostAndPort("192.168.2.47", 6379);
    private PooledConnectionProvider provider = new PooledConnectionProvider(config);
    private UnifiedJedis client = new UnifiedJedis(provider);
    private Object obj;
    private Iterator<JsonElement> variables;
    private JsonObject json,redis_fields;
   // @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfig() throws Exception{
        return new KafkaStreamsConfiguration(Map.of(
                APPLICATION_ID_CONFIG, "IceStreaming",
                BOOTSTRAP_SERVERS_CONFIG, "192.168.2.47:29092",
                DEFAULT_KEY_SERDE_CLASS_CONFIG, String.class,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest"
        ));
    }

    //@Bean
    public KStream<String, Weather> kstream(StreamsBuilder builder)throws Exception{
        Map<String, Object> serdeProps = new HashMap<>();

        final Serializer weatherSerializer = new CustomSerializer();
        weatherSerializer.configure(serdeProps, false);

        final Deserializer weatherDeserializer = new CustomDeserializer();
        weatherDeserializer.configure(serdeProps, false);

        final Serde weatherSerde = Serdes.serdeFrom(weatherSerializer, weatherDeserializer);

        KStream <String,Weather>stream = builder.stream("weather-data", Consumed.with(Serdes.String(),weatherSerde).withTimestampExtractor(new WeatherTimeExtractor()));
        stream.groupByKey().windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(20)));

       //stream.groupByKey().windowedBy(TimeWindows.ofSizeWithNoGrace(
          //switch()));

        KStream <String,Weather>m = stream.filter((key,value) -> {
            boolean isValidRecord = false;
           try {
               set = value.json().keys();
               while (set.hasNext()){
                   String valu = set.next();
                   map.put(valu, value.json().get(valu).getClass().getSimpleName());
               }
               System.out.println("Id "+value.id);
               obj = client.jsonGet("id_"+ value.id);

               if(obj.toString() != null){
                   json = JsonParser.parseString(obj.toString()).getAsJsonObject();
                   if(map.size() == json.getAsJsonArray("schema").size()){                                                             //to Make sure the number of fields sent is the required num in the db
                       variables = json.getAsJsonArray("schema").iterator();                                    //get Iterator for all json element in schema

                       while(variables.hasNext()){                                                                          //while the loop through them
                           redis_fields = variables.next().getAsJsonObject();   //Get the each OBject one after the other

                           if(map.containsKey(redis_fields.get("name").getAsString())
                                   && !map.get(redis_fields.get("name").getAsString())
                                   .equals(redis_fields.get("type").getAsString())) {              // if the field and name doesn't exist we out
                               return isValidRecord;
                           }
                       }
                   }
                   else {
                       return isValidRecord;
                   }
                   isValidRecord=true;
               }
               return isValidRecord;
           }
           catch (JSONException e){
               System.err.println("Could not deserialize record: " + e.getMessage());
           }
           catch (NullPointerException e){
               System.err.println("Could not find ID {"+ value.id+"} record in DB: " + e.getMessage());
           }
            return isValidRecord;
        });

        m.peek((key, value) -> System.out.println("Incoming record 1 - key " +key +" value " + value));

        //stream.peek((key, value) -> System.out.println("Incoming record 1 - key " +key +" value " + value.json()));

        return stream;
    }
}
