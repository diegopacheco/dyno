package com.netflix.dyno.jedis;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.netflix.config.ConfigurationManager;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.connectionpool.impl.RetryNTimes;
import com.netflix.dyno.connectionpool.impl.builder.DynomiteNodeInfo;
import com.netflix.dyno.connectionpool.impl.builder.DynomiteSeedsParser;
import com.netflix.dyno.connectionpool.impl.builder.HostSupplierFactory;
import com.netflix.dyno.connectionpool.impl.builder.TokenMapSupplierFactory;
import com.netflix.dyno.contrib.ArchaiusConnectionPoolConfiguration;

import redis.clients.jedis.Response;

public class DynoJedisPipelineTest {
	
	@Test
	public void testSimplePipeline() throws Throwable{
		
		ConfigurationManager.getConfigInstance().setProperty("dyno.dynomiteCluster.retryPolicy","RetryNTimes:3:true");
		
		String seeds = "172.18.0.101:8101:rack1:dc:100|172.18.0.102:8101:rack2:dc:100|172.18.0.103:8101:rack3:dc:100";

		List<DynomiteNodeInfo> nodes = DynomiteSeedsParser.parse(seeds);
		TokenMapSupplier tms = TokenMapSupplierFactory.build(nodes);
		HostSupplier hs = HostSupplierFactory.build(nodes);
		
		DynoJedisClient dynoClient = new DynoJedisClient.Builder().withApplicationName("dynomiteCluster")
				.withDynomiteClusterName("dynomiteCluster")
				.withCPConfig(new ArchaiusConnectionPoolConfiguration("dynomiteCluster")
						 	.withTokenSupplier(tms)
						    .setMaxConnsPerHost(1)
						    .setConnectTimeout(2000)
						    .setLocalRack("rack1")
						    .setRetryPolicyFactory(
								new RetryNTimes.RetryFactory(3,true)
				))
				.withHostSupplier(hs).build();
		
		DynoJedisPipeline pipe = dynoClient.pipelined();

		Response<String> result = pipe.get("k1");
		pipe.sync();
		pipe.close();
		
		String finalResult = result.get();
		Assert.assertNotNull(finalResult);
		Assert.assertEquals("1",finalResult);
		
	}
	
}
