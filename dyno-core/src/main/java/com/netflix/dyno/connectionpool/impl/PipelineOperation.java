package com.netflix.dyno.connectionpool.impl;

import com.netflix.dyno.connectionpool.Connection;
import com.netflix.dyno.connectionpool.Operation;

/**
 * Defines a specific callback for pipeline operations.
 * 
 * @author diegopacheco
 *
 * @param <CL> client
 * @param <R> result
 */
public interface PipelineOperation<CL, R> extends Operation<CL, R>{
	public Connection<CL> getConnection();
	public void setConnection(Connection<CL> connection);
}
