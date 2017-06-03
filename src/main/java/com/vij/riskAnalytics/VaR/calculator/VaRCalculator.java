package com.vij.riskAnalytics.VaR.calculator;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import com.google.gson.Gson;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import com.vij.riskAnalytics.Constants;
import com.vij.riskAnalytics.VaR.whatIf.WhatIfQuery;
import com.vij.riskAnalytics.cli.CommandLineUtil;
import com.vij.riskAnalytics.hierarchy.store.cache.HierarchyDataCacheStore;
import com.vij.riskAnalytics.ignite.CacheConnection;
import com.vij.riskAnalytics.simulations.StockSimulationsReaderFactory;
import com.vij.riskAnalytics.tradeDataStore.cache.TradeData;
import com.vij.riskAnalytics.tradeDataStore.cache.TradeDataCacheStore;
import com.vij.riskAnalytics.tradeDataStore.cache.TradeDataFromCache;

import scala.Tuple2;
import spark.jobserver.JavaSparkJob;

public class VaRCalculator extends JavaSparkJob implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger log = Logger.getLogger(VaRCalculator.class.getName());
	public VaRCalculator(){
		
	}
	
	
	public String runVaRQuery(JavaSparkContext jsc,VaRQuery vaRQuery )
	{
		CacheConnection.startIgnition();
		log.log(Level.INFO, "Running Var Query for business date ->"+vaRQuery.getBusinessDate());
		log.log(Level.INFO, "Running Var Query for portfolio ->"+vaRQuery.getPortfolioName());
		//CacheConnection.startIgnition();
		
		List<TradeDataFromCache> tradeDataList = getTradeData(vaRQuery);
		Double vaR = runVaRQuery(jsc,tradeDataList,vaRQuery.getBusinessDate());
		
		CacheConnection.stopIgnition();
		return "95% VaR "+ vaR;
		//jsc.close();
	}
	
	private List<TradeDataFromCache>  getTradeData(VaRQuery vaRQuery){
		
		TradeDataCacheStore tradeDataCacheStore = new TradeDataCacheStore();
		List<TradeDataFromCache> tradeDataList = null;
		if(vaRQuery.getHierarchy() == null)
		{
			 tradeDataList = tradeDataCacheStore.getTradeData(vaRQuery.getBusinessDate(), vaRQuery.getPortfolioName());
		}else{
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
			String date = vaRQuery.getBusinessDate().format(formatter);
			List<String> portfolios = HierarchyDataCacheStore.getInstance().givePortfolios(date,vaRQuery.getHierarchy());
			
			for(String portfolio : portfolios){
				log.log(Level.INFO, "Fecthing Trade data for Portfolio "+ portfolio);
				if(tradeDataList == null)
				{
					tradeDataList = tradeDataCacheStore.getTradeData(vaRQuery.getBusinessDate(), portfolio);
				}else
				{
				tradeDataList.addAll(tradeDataCacheStore.getTradeData(vaRQuery.getBusinessDate(), portfolio));
				}
			}
		}
		System.out.println(tradeDataList.size());
		return tradeDataList;
	}
	
	
	public Double runVaRQuery(JavaSparkContext jsc,List<TradeDataFromCache> tradeDataList,LocalDate businessDate){
		
		JavaRDD<Double> sortedByPortfolioReturnRDD = getPortfolioReturnRDD(jsc,tradeDataList,businessDate);
		//sortedByPortfolioReturnRDD.collect();
		List<Double> topLosses = sortedByPortfolioReturnRDD.takeOrdered((int) Math.max(Constants.NO_OF_SIMULATIONS/20, 1));
		
		if(topLosses.size()!=0)
		{
			log.log(Level.INFO,"Top Loss "+topLosses.get(0));
			log.log(Level.INFO,"95% VaR "+topLosses.get(topLosses.size()-1));
		}
		
		return topLosses.get(topLosses.size()-1);

	}

	public JavaRDD<Double> getPortfolioReturnRDD(JavaSparkContext jsc,List<TradeDataFromCache> tradeDataList,LocalDate businessDate){
		JavaPairRDD<String,Double> simulationReturnPairedRDD = null;
		log.log(Level.INFO, "Running Var Query for business date ->"+businessDate);
		for(TradeDataFromCache tradeDataForSingleStock : tradeDataList)
		{
			log.log(Level.INFO, "Getting simulations for stock ->"+tradeDataForSingleStock.getUnderlyer());
			
			if(simulationReturnPairedRDD == null)
			{
				simulationReturnPairedRDD = getSimulationsForStock(jsc,tradeDataForSingleStock.getUnderlyer(),businessDate)
						.mapToPair(row-> new Tuple2<String,Double>((String)row.get(0),(Double)row.get(1)*tradeDataForSingleStock.getNetInvestment()));
			}
			else
			{
				
				simulationReturnPairedRDD = simulationReturnPairedRDD
						.union(
								getSimulationsForStock(jsc,tradeDataForSingleStock.getUnderlyer(),businessDate)
				
								.mapToPair(row-> new Tuple2<String,Double>((String)row.get(0),(Double)row.get(1)*tradeDataForSingleStock.getNetInvestment())));
			}
		}
		
		JavaPairRDD<String,Double> groupBYSimulationRDD = simulationReturnPairedRDD.coalesce(2).reduceByKey(Double:: sum,4);
		JavaPairRDD<Double,String> sortedByPortfolioReturnPairedRDD = groupBYSimulationRDD
																	.mapToPair(pair->new Tuple2<>(pair._2,pair._1))
																	.sortByKey();
		JavaRDD<Double> sortedByPortfolioReturnRDD = sortedByPortfolioReturnPairedRDD.keys();
		//sortedByPortfolioReturnRDD.collect();
		
		
		return sortedByPortfolioReturnRDD;

	}

	private JavaRDD<Row> getSimulationsForStock(JavaSparkContext jsc,String stock,LocalDate businessDate)
	{
		DataFrame df =  StockSimulationsReaderFactory.getStockSimulationStore(jsc, businessDate, businessDate).getStockSimulationReturnsDataFrame(businessDate, stock);
		return df.javaRDD();
	}
	
	@Override
	  public Object runJob(JavaSparkContext jsc, Config jobConfig) {
		
		ConfigValue cv = jobConfig.getValue("input.string");
		 Gson gsonObj = new Gson();
		 VaRQuery varQuery = gsonObj.fromJson(cv.render(), VaRQuery.class);
		 log.log(Level.INFO,varQuery.toString());
	
		return runVaRQuery(jsc,varQuery);
	   
	  }
	
	public static void main(String args[])
	{
		//log.log(Level.INFO,args[0]+" -> "+args[1]);
		CommandLineUtil cli = new CommandLineUtil(args);
		 SparkConf conf = new SparkConf();
		/* conf.set("spark.shuffle.blockTransferService", "nio");*/
		 conf.setAppName("VaR Calculator");
		 if(!(System.getProperty("env", "aws")).equals("aws")){
			 conf.setMaster("local[3]");
			 conf.setExecutorEnv("spark.executor.memory", "1g");
			 }
		
		 VaRCalculator calculator = new VaRCalculator();
		 VaRQuery vaRQuery = new VaRQuery(cli.getPortfolio(), null, cli.getBusinessDate(), null, null,cli.getHierarchy());
		 Gson gsonObj = new Gson();
		 String str =  gsonObj.toJson(vaRQuery);
		 System.out.println("---->> "+str);
		 Config config = ConfigFactory.parseString("input.string = "+str);
		 calculator.runJob(new JavaSparkContext(conf),config);
		 
		
	}
}
