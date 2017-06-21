package com.netflix.dyno.jedis;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * This is a simple Reflection cache for Method used to re-call all pipeline ops in case of fallback.
 * 
 * @author diegopacheco
 *
 */
public class ReflectionCache {

	private Map<String,Method> cache = new HashMap<>();
	
	public ReflectionCache() {}
	
	public void add(String key,Method value){
		cache.put(key, value);
	}
	
	public Method get(String key){
		return cache.get(key);
	}
	
}
