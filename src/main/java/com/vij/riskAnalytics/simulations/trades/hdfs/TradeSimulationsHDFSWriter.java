package com.vij.riskAnalytics.simulations.trades.hdfs;

import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Locale;
import java.util.logging.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.kitesdk.data.Formats;

import com.vij.riskAnalytics.Constants;
import com.vij.riskAnalytics.IgniteCacheReaderWriter;
import com.vij.riskAnalytics.KiteSDKHDFSWriter;
import com.vij.riskAnalytics.ignite.CacheConnection;
import com.vij.riskAnalytics.simulations.StockSimulationsReaderFactory;
import com.vij.riskAnalytics.simulations.cache.StockSimulationsCacheReader.StockSimulation;
import com.vij.riskAnalytics.tradeDataStore.cache.TradeDataCacheStore;
import com.vij.riskAnalytics.tradeDataStore.cache.TradeDataFromCache;

import scala.Tuple2;
import spark.jobserver.JavaSparkJob;


public class TradeSimulationsHDFSWriter extends JavaSparkJob implements Serializable{

	private static final long serialVersionUID = 1L;
	private static final Logger log = Logger.getLogger(TradeSimulationsHDFSWriter.class.getName());
	
	public static void main(String args[])
	{
		 SparkConf conf = new SparkConf();
		 conf.setAppName("TradeSimulationHDFSWriter");
		 if(!(System.getProperty("env", "aws")).equals("aws")){
			 conf.setMaster("local[3]");
			 conf.setExecutorEnv("spark.executor.memory", "1g");
			 }
		
		 CacheConnection.startIgnition();
		 TradeSimulationsHDFSWriter hdfsWriter = new TradeSimulationsHDFSWriter();  
		 hdfsWriter.writeToHFS(new JavaSparkContext(conf),args[0],Constants.BASE_TRADE_SIMULATION_DIR);
			
		 CacheConnection.stopIgnition();
		

		 
		
	}
	
	private  void writeToHFS(JavaSparkContext javaSparkContext,String businessDateStr,String outPutDir) {
		int businessDateInt = Integer.parseInt(businessDateStr);
		LocalDate businessDate	= LocalDate.parse(businessDateStr, DateTimeFormatter.ofPattern("yyyyMMdd",Locale.ENGLISH));
		JavaRDD<TradeDataFromCache> tradeDataRDD = getTradeDataRDD(businessDate,javaSparkContext);
		JavaRDD<StockSimulation> stockSimulationRDD = getStockSimulationRDD(businessDate,javaSparkContext);
	
		 JavaPairRDD<String,TradeDataFromCache> tradeDataPairedRDD = tradeDataRDD.mapToPair(f-> new Tuple2(f.getUnderlyer(),f));
		 JavaPairRDD<String,StockSimulation> stockSimulatioPairedRDD = stockSimulationRDD.mapToPair(r-> new Tuple2(r.getSymbol(),r));
		 JavaPairRDD<String, Tuple2<Iterable<TradeDataFromCache>, Iterable<StockSimulation>>> tradeStockSimulatioPairedItrRDD = tradeDataPairedRDD.cogroup(stockSimulatioPairedRDD);
		
		 
		 JavaPairRDD<String, TradeSimulation> tradeSimulationsPairedRDD =  tradeStockSimulatioPairedItrRDD.flatMapValues(new Function<Tuple2<Iterable<TradeDataFromCache>,Iterable<StockSimulation>>,Iterable<TradeSimulation>>() {
			@Override
			public Iterable<TradeSimulation> call(Tuple2<Iterable<TradeDataFromCache>, Iterable<StockSimulation>> f) throws Exception {
				Iterable<TradeDataFromCache> tradeDataIterable = f._1();
				Iterable<StockSimulation> stockSimulatedIterable = f._2();
				ArrayList<TradeSimulation> tradeDataSimulations = new ArrayList<TradeSimulation>();
				for(TradeDataFromCache tradeData : tradeDataIterable)
				{
					for(StockSimulation stockSimulation : stockSimulatedIterable)
					{
						tradeDataSimulations.add(
								TradeSimulation.newBuilder()
								.setDealId(tradeData.getDealId())
								.setSimulationId(stockSimulation.getSimulationID())
								.setUnderlyer(tradeData.getUnderlyer())
								.setPortfolio(tradeData.getPortfolio())
								.setPosition(tradeData.getNetInvestment())
								.setSimulatedPnLAmt(tradeData.getNetInvestment()*stockSimulation.getStockReturn())
								.setBusinessDate(businessDateInt).build());
					}
				}
				
				return tradeDataSimulations;
			}
		});
		 
		 JavaPairRDD<TradeSimulationKey, TradeSimulation> tradeSimulationsKeyPairedRDD = tradeSimulationsPairedRDD.mapToPair(f-> new Tuple2<TradeSimulationKey,TradeSimulation>(new TradeSimulationKey(f._2.getSimulationId(), f._2.getDealId(), f._2.getBusinessDate()),f._2 ));
		 //JavaRDD<TradeSimulation> tradeSimulationsRDD = tradeSimulationsPairedRDD.values();
		 //JavaPairRDD<TradeSimulationKey, TradeSimulation> tradeSimulationsKeyPairedRDD = tradeSimulationsRDD.mapToPair(f-> new Tuple2<TradeSimulationKey,TradeSimulation>(new TradeSimulationKey(f.getSimulationId(), f.getDealId(), f.getBusinessDate()),f ));
		  IgniteCacheReaderWriter<TradeSimulationKey, TradeSimulation> cacheHandle = new IgniteCacheReaderWriter<TradeSimulationKey, TradeSimulation>(javaSparkContext,TradeSimulationKey.class,TradeSimulation.class,"TradeSimulationsCache");
		  cacheHandle.put(tradeSimulationsKeyPairedRDD);
		  try {
			KiteSDKHDFSWriter.write(TradeSimulation.class, Formats.PARQUET, outPutDir, tradeSimulationsKeyPairedRDD);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	private JavaRDD<TradeDataFromCache>  getTradeDataRDD(LocalDate businessDate,JavaSparkContext javaSparkContext){
		
		TradeDataCacheStore tradeDataCacheStore = new TradeDataCacheStore(javaSparkContext);
		return tradeDataCacheStore.getTradeDataRDD(businessDate);
		
	}
	
	private JavaRDD<StockSimulation>  getStockSimulationRDD(LocalDate businessDate,JavaSparkContext javaSparkContext){
		
	return  StockSimulationsReaderFactory.getStockSimulationStore(javaSparkContext, businessDate, businessDate).getStockSimulationReturnsRDD(businessDate);
		
	}

}
