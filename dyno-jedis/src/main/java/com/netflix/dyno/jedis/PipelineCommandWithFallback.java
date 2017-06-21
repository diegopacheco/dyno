package com.netflix.dyno.jedis;

import redis.clients.jedis.Response;

/**
 * Execute pipeline operation with safe fallback and retrys
 * 
 * @author diegopacheco
 *
 * @param <T>
 */
public interface PipelineCommandWithFallback<T> {
	public Response<T> execute();
}
