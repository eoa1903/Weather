package com.dayo.weather;

import com.dayo.weather.kafkaservice.Producer;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.json.Path;
import redis.clients.jedis.providers.PooledConnectionProvider;

@RestController
@SpringBootApplication
public class WeatherApplication {
	@Autowired
	Producer producer;



	public static void main(String[] args) {
		SpringApplication.run(WeatherApplication.class, args);

	}

	@GetMapping("/home")
	public String home() {
		HostAndPort config = new HostAndPort("192.168.2.47", 6379);
		PooledConnectionProvider provider = new PooledConnectionProvider(config);
		UnifiedJedis client = new UnifiedJedis(provider);

		int i=4;
		while(i<=6) {
			JsonObject obj = new JsonObject();
			obj.addProperty("id", i);
//			//if( i%2 == 0){
//				obj.addProperty("policy_time_name", "secs");
//				obj.addProperty("policy_time_value", 1);
//			//}
			if( i == 4){
				obj.addProperty("policy_time_name", "mins");
				obj.addProperty("policy_time_value", 1);
			}
			if( i == 5){
				obj.addProperty("policy_time_name", "hours");
				obj.addProperty("policy_time_value", 1);
			}
			if( i == 6){
				obj.addProperty("policy_time_name", "days");
				obj.addProperty("policy_time_value", 1);
			}

			JsonArray jsonArray = new JsonArray();
			JsonObject obj_sch0 = new JsonObject();
			obj_sch0.addProperty("name","id");
			obj_sch0.addProperty("type", "Integer");
			jsonArray.add(obj_sch0);
			JsonObject obj_sch1 = new JsonObject();
			obj_sch1.addProperty("name","lat");
			obj_sch1.addProperty("type", "Double");
			jsonArray.add(obj_sch1);
			JsonObject obj_sch2 = new JsonObject();
			obj_sch2.addProperty("name","lon");
			obj_sch2.addProperty("type", "Double");
			jsonArray.add(obj_sch2);
			JsonObject obj_sch3 = new JsonObject();
			obj_sch3.addProperty("name","phyQt");
			obj_sch3.addProperty("type", "String");
			jsonArray.add(obj_sch3);
			JsonObject obj_sch4 = new JsonObject();
			obj_sch4.addProperty("name","timestamp");
			obj_sch4.addProperty("type", "Long");
			jsonArray.add(obj_sch4);
			obj.add("schema",jsonArray);

			client.jsonSet("id_"+i, Path.ROOT_PATH, obj);
			i++;
		}

		return "Hello Dayo!";
	}
}
