package com.vij.riskAnalytics.marketDataStore.cache;


import java.time.LocalDate;
import java.util.Date;
import java.util.Map;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;

import com.vij.riskAnalytics.ignite.CacheConnection;

public class MarketDataCacheStore {
	
	
	private MarketDataCacheStore(){
		mktDataCacheCfg = new CacheConfiguration<>(PARTITIONED_CACHE_NAME);
		mktDataCacheCfg.setBackups(1);
		mktDataCacheCfg.setCacheMode(CacheMode.PARTITIONED);
		mktDataCacheCfg.setIndexedTypes(MarketDataKey.class,MarketData.class);
		Ignite ignite = Ignition.ignite("riskAnalyticsGrid");
		mktDataCache = ignite.getOrCreateCache(mktDataCacheCfg);
	}

	private IgniteCache<MarketDataKey, MarketData> mktDataCache = null;
	private static MarketDataCacheStore cacheStore = new MarketDataCacheStore();
	
	public static MarketDataCacheStore getInstance(){
		return cacheStore;
	}
	private static final String PARTITIONED_CACHE_NAME = "MarketDataCache";
	private CacheConfiguration<MarketDataKey, MarketData> mktDataCacheCfg = null;
	
	public void putMktDataInCache(Map<MarketDataKey,MarketData> marketDataMap){
		mktDataCache.putAll(marketDataMap);
	}
	public void putMktDataInCache(MarketDataKey marketDataKey,MarketData marketData){
		mktDataCache.put(marketDataKey,marketData);
	}
	
	public Double[] getMktData(String symbol,LocalDate startDate,LocalDate endDate)
	{
		String sql = "symbol = ? and businessDate between ? and ? order by businessDate";
				 
				 System.out.println("MarketData for symbol :"+symbol+" with COB between "+startDate.toString()+ "and "+endDate.toString() +"queried with SQL query): ");
						 Double [] mktDataForPeriod =  mktDataCache.query(new SqlQuery<MarketDataKey, MarketData>(MarketData.class, sql).
				                setArgs(symbol, startDate,endDate)).getAll().stream().map(obj-> {
				                	MarketData mktData = obj.getValue();
				                	return mktData.getEodPrice();
				                }).toArray(Double[]::new);
						 
						 System.out.println("Size returned "+mktDataForPeriod.length);
						 return mktDataForPeriod;
		}
		
		
				 
	

}
