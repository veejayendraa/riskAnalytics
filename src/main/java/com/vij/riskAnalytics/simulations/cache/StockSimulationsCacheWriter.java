package com.vij.riskAnalytics.simulations.cache;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.fair.FairAffinityFunction;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.spark.IgniteContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.vij.riskAnalytics.Constants;

public class StockSimulationsCacheWriter {

	private IgniteContext<StockSimulationReturnsKey, StockSimulationReturnsVal> ic;
	private static final Logger log = Logger.getLogger(StockSimulationsCacheWriter.class.getName());
	public StockSimulationsCacheWriter(JavaSparkContext jsc)
	{
		this();
		String config = null;
		String env = System.getProperty("env", "aws");
		if(env.equals("aws")){
			log.log(Level.INFO, ">>> RUNNING IN AWS ENVIRONMENT <<<");
			  config = Constants.AWS_CLIENT_CACHE_CONFIG_XML;
		}else{
			log.log(Level.INFO, ">>> RUNNING IN LOCAL ENVIRONMENT <<<");
			config = Constants.LOCAL_CLIENT_CACHE_CONFIG_XML;
		}
	/*
	* When creating an IgniteContext instance, an optional boolean client argument (defaulting to true)
	* can be passed to context constructor.This is typically used in a Shared Deployment installation.
	* When client is set to false, context will operate in embedded mode and will start server nodes on all workers during the context construction.
	*/
		this.ic = new IgniteContext<StockSimulationReturnsKey,StockSimulationReturnsVal>(jsc.sc(), config,true);
	}
	private IgniteCache<StockSimulationReturnsKey, StockSimulationReturnsVal> stockSimulationsDataCache = null;
	private static final String PARTITIONED_CACHE_NAME = StockSimulationsCacheWriter.class.getSimpleName() + "Partitioned";
	private CacheConfiguration<StockSimulationReturnsKey, StockSimulationReturnsVal> stockSimulationsDataCacheCfg = new CacheConfiguration<>(PARTITIONED_CACHE_NAME);
	
	public StockSimulationsCacheWriter(){
		stockSimulationsDataCacheCfg.setBackups(1);
		stockSimulationsDataCacheCfg.setCacheMode(CacheMode.PARTITIONED);
		stockSimulationsDataCacheCfg.setIndexedTypes(StockSimulationReturnsKey.class,StockSimulationReturnsVal.class);
		stockSimulationsDataCacheCfg.setName(PARTITIONED_CACHE_NAME);
		RendezvousAffinityFunction  affinityFunction = new RendezvousAffinityFunction ();
		stockSimulationsDataCacheCfg.setAffinity(affinityFunction);
		stockSimulationsDataCache = Ignition.ignite("riskAnalyticsGrid").getOrCreateCache(stockSimulationsDataCacheCfg);
		
	}
	
	
	
	public void putMktDataInCache(Map<StockSimulationReturnsKey,StockSimulationReturnsVal> stockSimulationsMap){
		stockSimulationsDataCache.putAll(stockSimulationsMap);
		
	}
	
	public void putMktDataInCache(JavaPairRDD<StockSimulationReturnsKey,StockSimulationReturnsVal> rdd){
		ic.fromCache(stockSimulationsDataCacheCfg).savePairs(rdd.rdd(),true);
	}
	
	public void putMktDataInCache(StockSimulationReturnsKey stockSimDataKey,StockSimulationReturnsVal stockSimData){
		stockSimulationsDataCache.put(stockSimDataKey,stockSimData);
	}
	
}
