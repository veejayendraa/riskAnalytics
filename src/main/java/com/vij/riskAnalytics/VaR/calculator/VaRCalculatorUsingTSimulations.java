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
import com.vij.riskAnalytics.IgniteCacheReaderWriter;
import com.vij.riskAnalytics.VaR.whatIf.WhatIfQuery;
import com.vij.riskAnalytics.cli.CommandLineUtil;
import com.vij.riskAnalytics.hierarchy.store.cache.HierarchyDataCacheStore;
import com.vij.riskAnalytics.ignite.CacheConnection;
import com.vij.riskAnalytics.simulations.StockSimulationsReaderFactory;
import com.vij.riskAnalytics.simulations.trades.hdfs.TradeSimulation;
import com.vij.riskAnalytics.simulations.trades.hdfs.TradeSimulationKey;
import com.vij.riskAnalytics.simulations.trades.hdfs.TradeSimulationsHDFSReader;
import com.vij.riskAnalytics.tradeDataStore.cache.TradeData;
import com.vij.riskAnalytics.tradeDataStore.cache.TradeDataCacheStore;
import com.vij.riskAnalytics.tradeDataStore.cache.TradeDataFromCache;

import scala.Tuple2;
import spark.jobserver.JavaSparkJob;

public class VaRCalculatorUsingTSimulations extends JavaSparkJob implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger log = Logger.getLogger(VaRCalculatorUsingTSimulations.class.getName());
	private TradeSimulationsHDFSReader hdfsReader = null;
	public VaRCalculatorUsingTSimulations(){
		
	}
	
	
	public VaRResult runVaRQuery(JavaSparkContext jsc,VaRQuery vaRQuery,boolean readFromCache )
	{
		
		log.log(Level.INFO, "Running Var Query for business date ->"+vaRQuery.getBusinessDate());
		log.log(Level.INFO, "Running Var Query for portfolio ->"+vaRQuery.getPortfolioName());
		CacheConnection.startIgnition();
		
		List<TradeDataFromCache>  tradeDatalist = getTradeData(vaRQuery,jsc);
		if(hdfsReader == null)
		hdfsReader = new TradeSimulationsHDFSReader(vaRQuery.getBusinessDate(), jsc, Constants.BASE_TRADE_SIMULATION_DIR);
		VaRResult var = runVaRQuery(jsc,tradeDatalist,vaRQuery.getBusinessDate(),readFromCache);
		
		System.out.println(var);
		CacheConnection.stopIgnition();
		jsc.close();
		return var;
		//
	}
	
	private List<TradeDataFromCache>  getTradeData(VaRQuery vaRQuery,JavaSparkContext jsc){
		
		TradeDataCacheStore tradeDataCacheStore = new TradeDataCacheStore(jsc);
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
	
	
	public VaRResult runVaRQuery(JavaSparkContext jsc,List<TradeDataFromCache> tradeDatalist,LocalDate businessDate,boolean readFromCache){
		log.log(Level.INFO, "Running Var Query for business date ->"+businessDate);
		JavaRDD<Double> sortedByPortfolioReturnRDD = getPortfolioReturnRDD(tradeDatalist,jsc,businessDate,readFromCache);
		sortedByPortfolioReturnRDD = sortedByPortfolioReturnRDD.cache();
		List<Double> topLosses = sortedByPortfolioReturnRDD.takeOrdered((int) Math.max(Constants.NO_OF_SIMULATIONS/20, 1));
		
		
		if(topLosses.size()!=0)
		{
			log.log(Level.INFO,"Top Loss "+topLosses.get(0));
			log.log(Level.INFO,"95% VaR "+topLosses.get(topLosses.size()-1));
		}
		
		return new VaRResult(topLosses.get(topLosses.size()-1), sortedByPortfolioReturnRDD.collect());

	}

	public JavaRDD<Double> getPortfolioReturnRDD(List<TradeDataFromCache> tradeDatalist,JavaSparkContext jsc,LocalDate businessDate, boolean readFromCache){
	
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
		String businessDateStr = businessDate.format(formatter);
		
		 IgniteCacheReaderWriter<TradeSimulationKey, TradeSimulation> cacheHandle = new IgniteCacheReaderWriter<TradeSimulationKey, TradeSimulation>(jsc,TradeSimulationKey.class,TradeSimulation.class,"TradeSimulationsCache");
		JavaPairRDD<String,Double> simulationReturnPairedRDD = null;
		
		for(TradeDataFromCache tradeDataForSingleTrade : tradeDatalist)
		{
			log.log(Level.INFO, "Getting simulations for trade ->"+tradeDataForSingleTrade.getDealId());
			if(simulationReturnPairedRDD == null)
			{
				JavaRDD<TradeSimulation> tradeSimulation = getSimulationsForTrade(cacheHandle,tradeDataForSingleTrade.getDealId(),tradeDataForSingleTrade.getPortfolio(),businessDateStr,readFromCache);
				simulationReturnPairedRDD = tradeSimulation.mapToPair(f->new Tuple2<String,Double>(f.getSimulationId(),f.getSimulatedPnLAmt()));
			}else{
				simulationReturnPairedRDD
					.union(
							getSimulationsForTrade(cacheHandle,tradeDataForSingleTrade.getDealId(),tradeDataForSingleTrade.getPortfolio(),businessDateStr,readFromCache)
								.mapToPair(f->new Tuple2<String,Double>(f.getSimulationId(),f.getSimulatedPnLAmt()))
						   );
			}
			
		}
		
		JavaPairRDD<String,Double> groupBYSimulationRDD = simulationReturnPairedRDD.coalesce(2).reduceByKey(Double:: sum,4);
		JavaPairRDD<Double,String> sortedByPortfolioReturnPairedRDD = groupBYSimulationRDD
																	.mapToPair(pair->new Tuple2<>(pair._2,pair._1))
																	.sortByKey();
		JavaRDD<Double> sortedByPortfolioReturnRDD = sortedByPortfolioReturnPairedRDD.keys();
		return sortedByPortfolioReturnRDD;

	}

	
	
	private JavaRDD<TradeSimulation> getSimulationsForTrade(IgniteCacheReaderWriter<TradeSimulationKey, TradeSimulation> cacheHandle,String dealId,String book,String businessDate,boolean readFromCache)
	{
		if(readFromCache)
		{
			return cacheHandle.get(TradeSimulation.class, businessDate, book, dealId);
		}else{
		return hdfsReader.getTradeSimulationsRDD(dealId,book);
		}
	}
	
	@Override
	  public Object runJob(JavaSparkContext jsc, Config jobConfig) {
		
		ConfigValue cv = jobConfig.getValue("input.string");
		 Gson gsonObj = new Gson();
		 VaRQuery varQuery = gsonObj.fromJson(cv.render(), VaRQuery.class);
		 log.log(Level.INFO,varQuery.toString());
		 
		 System.out.println("Running actual query**********************************************************************************");
		// runVaRQuery(jsc,varQuery,true);
		// System.out.println("******************************************Done for once *********************************************");
		 VaRResult result = runVaRQuery(jsc,varQuery,false);
		// System.out.println("******************************************Done for twice *********************************************");
		 System.out.println(result.getVar()+" Simulations "+result.getSimulations().size());
	/*	try {
		Thread.sleep(10000000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		return result;
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
		
		 VaRCalculatorUsingTSimulations calculator = new VaRCalculatorUsingTSimulations();
		 VaRQuery vaRQuery = new VaRQuery(cli.getPortfolio(), null, cli.getBusinessDate(), null, null,cli.getHierarchy());
		 Gson gsonObj = new Gson();
		 String str =  gsonObj.toJson(vaRQuery);
		 System.out.println("---->> "+str);
		 Config config = ConfigFactory.parseString("input.string = "+str);
		Object var = calculator.runJob(new JavaSparkContext(conf),config);
		System.out.println(var); 
		 
		
	}
}
