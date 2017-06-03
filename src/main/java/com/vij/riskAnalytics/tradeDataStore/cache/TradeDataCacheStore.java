package com.vij.riskAnalytics.tradeDataStore.cache;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;

import com.vij.riskAnalytics.Constants;
import com.vij.riskAnalytics.VaR.calculator.VaRCalculator;
import com.vij.riskAnalytics.simulations.cache.StockSimulationsCacheReader;


public class TradeDataCacheStore implements Serializable{
	
	private IgniteCache<TradeDataKey, TradeData> tradeDataCache = null;
	private static final String PARTITIONED_CACHE_NAME = TradeDataCacheStore.class.getSimpleName() + "Partitioned";
	private CacheConfiguration<TradeDataKey, TradeData> tradeDataCacheCfg = new CacheConfiguration<>(PARTITIONED_CACHE_NAME);
	private static final Logger log = Logger.getLogger(TradeDataCacheStore.class.getName());
	private JavaIgniteContext<String, String> jic;
	public TradeDataCacheStore(){
		tradeDataCacheCfg.setBackups(1);
		tradeDataCacheCfg.setCacheMode(CacheMode.PARTITIONED);
		tradeDataCacheCfg.setIndexedTypes(TradeDataKey.class,TradeData.class);
		//riskAnalyticsGrid is gridName.Ideally should have come from some property file.
		tradeDataCache = Ignition.ignite("riskAnalyticsGrid").getOrCreateCache(tradeDataCacheCfg);
		
	}
	
	public TradeDataCacheStore(JavaSparkContext jsc){
		tradeDataCacheCfg.setBackups(1);
		tradeDataCacheCfg.setCacheMode(CacheMode.PARTITIONED);
		tradeDataCacheCfg.setIndexedTypes(TradeDataKey.class,TradeData.class);
		//riskAnalyticsGrid is gridName.Ideally should have come from some property file.
		tradeDataCache = Ignition.ignite("riskAnalyticsGrid").getOrCreateCache(tradeDataCacheCfg);
		if(System.getProperty("env","aws").equals("aws"))
		{
			log.log(Level.INFO, ">>> RUNNING IN AWS ENVIRONMENT <<<");
			jic = new JavaIgniteContext<>(jsc, Constants.AWS_CLIENT_CACHE_CONFIG_XML);
		}
		else
		{
			log.log(Level.INFO, ">>> RUNNING IN LOCAL ENVIRONMENT <<<");
			jic = new JavaIgniteContext<>(jsc, Constants.LOCAL_CLIENT_CACHE_CONFIG_XML);
		}
	}
	
	
	public void putTradeDataInCache(Map<TradeDataKey,TradeData> tradeMap){
		tradeDataCache.putAll(tradeMap);
	}
	public void putTradeDataInCache(TradeDataKey tradeDataKey,TradeData tradeData){
		tradeDataCache.put(tradeDataKey,tradeData);
	}
	
	public List<TradeDataFromCache> getTradeData(LocalDate businessDate,String portfolio)
	{
		List<TradeDataFromCache> tradeDataList = new ArrayList<TradeDataFromCache>();
		String sql = "businessDate = ?";
		log.log(Level.INFO,"Get Trade Data for businessDate "+businessDate);
		tradeDataCache.query(new SqlQuery<TradeDataKey, TradeData>(TradeData.class, sql)
				.setArgs(businessDate))
				.getAll()
				.forEach( data-> tradeDataList.add(new TradeDataFromCache(data.getValue().getPortfolio(),data.getValue().getUnderlyer(),data.getValue().getNetInvestment(),data.getKey().getDealId())));
				 
		log.log(Level.INFO,"Size returned "+tradeDataList.size());
		return tradeDataList;
	}

	public List<TradeData> getTradeData(LocalDate businessDate)
	{
		List<TradeData> tradeDataList = new ArrayList<TradeData>();
		String sql = "businessDate = ?";
		log.log(Level.INFO,"Get Trade Data for businessDate "+businessDate);
		tradeDataCache.query(new SqlQuery<TradeDataKey, TradeData>(TradeData.class, sql)
				.setArgs(businessDate))
				.getAll()
				.forEach( data-> tradeDataList.add(data.getValue()));;
				 
		log.log(Level.INFO,"Size returned "+tradeDataList.size());
		return tradeDataList;
	}
	
	public JavaRDD<TradeDataFromCache> getTradeDataRDD(LocalDate businessDate)
	{
		
		String sql = "select PORTFOLIO,UNDERLYER,NETINVESTMENT,DEALID from TRADEDATA where businessDate = ?";
		DataFrame df =jic.fromCache(PARTITIONED_CACHE_NAME).sql(sql, businessDate);
		JavaRDD<TradeDataFromCache> javaRDD = df.javaRDD().map(f-> new TradeDataFromCache(f.getAs("PORTFOLIO"),f.getAs("UNDERLYER"),f.getAs("NETINVESTMENT"),f.getAs("DEALID")));
		return javaRDD;
	}
	
		



}
