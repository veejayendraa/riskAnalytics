package com.vij.riskAnalytics.test;

import java.time.LocalDate;

import com.vij.riskAnalytics.ignite.CacheConnection;
import com.vij.riskAnalytics.simulations.cache.StockSimulationReturnsKey;
import com.vij.riskAnalytics.simulations.cache.StockSimulationReturnsVal;
import com.vij.riskAnalytics.simulations.cache.StockSimulationsCacheWriter;

public class TestAffinity {

	public static void main(String args[])
	{
		CacheConnection.startIgnition();
		StockSimulationsCacheWriter cacheWriter = new StockSimulationsCacheWriter();
		LocalDate now = LocalDate.now();
		cacheWriter.putMktDataInCache(new StockSimulationReturnsKey(now, "ABC", "1"), new StockSimulationReturnsVal(12.0));
		cacheWriter.putMktDataInCache(new StockSimulationReturnsKey(now, "XYZ", "1"), new StockSimulationReturnsVal(121.0));
		cacheWriter.putMktDataInCache(new StockSimulationReturnsKey(now, "ABC", "2"), new StockSimulationReturnsVal(112.0));
		cacheWriter.putMktDataInCache(new StockSimulationReturnsKey(now, "XYZ", "2"), new StockSimulationReturnsVal(122.0));
		cacheWriter.putMktDataInCache(new StockSimulationReturnsKey(now, "ABC", "11"), new StockSimulationReturnsVal(112.0));
		cacheWriter.putMktDataInCache(new StockSimulationReturnsKey(now, "XYZ", "11"), new StockSimulationReturnsVal(142.0));
		cacheWriter.putMktDataInCache(new StockSimulationReturnsKey(now, "ABC", "21"), new StockSimulationReturnsVal(812.0));
		cacheWriter.putMktDataInCache(new StockSimulationReturnsKey(now, "XYZ", "21"), new StockSimulationReturnsVal(312.0));
		cacheWriter.putMktDataInCache(new StockSimulationReturnsKey(now, "ABC", "12"), new StockSimulationReturnsVal(912.0));
		cacheWriter.putMktDataInCache(new StockSimulationReturnsKey(now, "XYZ", "12"), new StockSimulationReturnsVal(102.0));
		cacheWriter.putMktDataInCache(new StockSimulationReturnsKey(now, "ABC", "22"), new StockSimulationReturnsVal(112.0));
		cacheWriter.putMktDataInCache(new StockSimulationReturnsKey(now, "XYZ", "22"), new StockSimulationReturnsVal(412.0));
		cacheWriter.putMktDataInCache(new StockSimulationReturnsKey(now, "ABC", "211"), new StockSimulationReturnsVal(12.0));
		cacheWriter.putMktDataInCache(new StockSimulationReturnsKey(now, "XYZ", "211"), new StockSimulationReturnsVal(152.0));
		cacheWriter.putMktDataInCache(new StockSimulationReturnsKey(now, "ABC", "221"), new StockSimulationReturnsVal(172.0));
		cacheWriter.putMktDataInCache(new StockSimulationReturnsKey(now, "XYZ", "221"), new StockSimulationReturnsVal(182.0));
		CacheConnection.stopIgnition();
	}
}
