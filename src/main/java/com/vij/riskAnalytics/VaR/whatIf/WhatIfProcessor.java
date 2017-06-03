package com.vij.riskAnalytics.VaR.whatIf;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.gson.Gson;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;
import com.vij.riskAnalytics.VaR.calculator.VaRCalculator;
import com.vij.riskAnalytics.VaR.calculator.VaRQuery;
import com.vij.riskAnalytics.cli.CommandLineUtil;
import com.vij.riskAnalytics.ignite.CacheConnection;
import com.vij.riskAnalytics.tradeDataStore.cache.TradeData;
import com.vij.riskAnalytics.tradeDataStore.cache.TradeDataCacheStore;
import com.vij.riskAnalytics.tradeDataStore.cache.TradeDataFromCache;

import spark.jobserver.JavaSparkJob;

public class WhatIfProcessor extends JavaSparkJob implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger log = Logger.getLogger(WhatIfProcessor.class.getName());
	
	public static void main(String[] args) {
		log.log(Level.INFO,"Business Date ->"+args[0]+" Portfolio -> "+args[1]+" Stock ->"+ args[2]+" Investment ->"+args[3]);

		 SparkConf conf = new SparkConf();
			/* conf.set("spark.shuffle.blockTransferService", "nio");*/
			 conf.setAppName("What-If");
			 if(!(System.getProperty("env", "aws")).equals("aws")){
				 conf.setMaster("local[3]");
				 conf.setExecutorEnv("spark.executor.memory", "1g");
				 }
			 
			 WhatIfProcessor whatIfProcessor = new WhatIfProcessor();
			 Map<String,Double> whatIfScenrios = new HashMap<String,Double>();
			 whatIfScenrios.put("ARAY", 20000d);
			 whatIfScenrios.put("YPRO", 20000d);
			 Gson gsonObj = new Gson();
			 String str =  gsonObj.toJson( new WhatIfQuery("2016-03-17","portfolio1",whatIfScenrios));
			 System.out.println("---->> "+str);
			 Config config = ConfigFactory.parseString("input.string = "+str);
			 whatIfProcessor.runJob(new JavaSparkContext(conf), config);
		
	}
	
	@Override
	  public Object runJob(JavaSparkContext jsc, Config jobConfig) {
		ConfigValue cv = jobConfig.getValue("input.string");
		 Gson gsonObj = new Gson();
		 WhatIfQuery whatIfQuery = gsonObj.fromJson(cv.render(), WhatIfQuery.class);
		 log.log(Level.INFO,whatIfQuery.toString());
		return whatIf(jsc, whatIfQuery);
	   
	  }
	public String whatIf(JavaSparkContext jsc,WhatIfQuery whatIfQuery)
	{
		CacheConnection.startIgnition();
		
		log.log(Level.INFO, "Running What-If for business date ->"+whatIfQuery.getBusinessDate());
		log.log(Level.INFO, "Running What-If for portfolio ->"+whatIfQuery.getPortfolio());
		
		for(Entry<String, Double> entry : whatIfQuery.getStockInvestmentMap().entrySet())
		{
			log.log(Level.INFO, "Running What-If for stock ->"+entry.getKey());
			log.log(Level.INFO, "Running What-If for investment ->"+entry.getValue());
		}
		
		
		TradeDataCacheStore tradeDataCacheStore = new TradeDataCacheStore();
		List<TradeDataFromCache> tradeDataList = tradeDataCacheStore.getTradeData(whatIfQuery.getBusinessDate(), whatIfQuery.getPortfolio());

		List<TradeDataFromCache> modifiedTradeDataList = doAdjustmentForWhatIfQuery(tradeDataList,whatIfQuery);
		VaRCalculator calculator = new VaRCalculator();
		Double whatIfVaR = calculator.runVaRQuery(jsc, modifiedTradeDataList, whatIfQuery.getBusinessDate());
		log.log(Level.INFO,"95% What-If VaR "+ whatIfVaR);
		
		CacheConnection.stopIgnition();
		
		return "95% What-If VaR "+ whatIfVaR;
	}
	
	private List<TradeDataFromCache> doAdjustmentForWhatIfQuery(List<TradeDataFromCache> tradeDataList,WhatIfQuery whatIfQuery){

		//boolean stockDataFound = false;
		Set<String> stocksFoundSet = new HashSet<String>();
		List<TradeDataFromCache> modifiedTradeDataList = new ArrayList<TradeDataFromCache>();
		for(TradeDataFromCache tradeData : tradeDataList){
			
			if(whatIfQuery.getStockInvestmentMap().get(tradeData.getUnderlyer())!=null){
				log.log(Level.INFO,"Changing stock positions of stock "+tradeData.getUnderlyer() +" for What-If query from "+tradeData.getNetInvestment() + "to "+whatIfQuery.getStockInvestmentMap().get(tradeData.getUnderlyer()));
				modifiedTradeDataList.add(new TradeDataFromCache(tradeData.getPortfolio(), tradeData.getUnderlyer(), whatIfQuery.getStockInvestmentMap().get(tradeData.getUnderlyer()),tradeData.getDealId()));
				stocksFoundSet.add(tradeData.getUnderlyer());
			}else{
				modifiedTradeDataList.add(tradeData);
			}
		}
		Set<String> allStocksInQuery = whatIfQuery.getStockInvestmentMap().keySet();
		allStocksInQuery.removeAll(stocksFoundSet);
		if( allStocksInQuery.size() != 0)
		{
			for(String stock : allStocksInQuery)
			{
			log.log(Level.INFO, "No positions found for Stock "+stock+" so adding "+whatIfQuery.getStockInvestmentMap().get(stock)+" positions for what-if query");
			modifiedTradeDataList.add(new TradeDataFromCache(whatIfQuery.getPortfolio(), stock, whatIfQuery.getStockInvestmentMap().get(stock),null));
			}
		}
		
		return modifiedTradeDataList;
	}

}
