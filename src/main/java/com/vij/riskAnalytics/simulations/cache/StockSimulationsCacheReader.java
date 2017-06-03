package com.vij.riskAnalytics.simulations.cache;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;

import com.vij.riskAnalytics.simulations.IStockSimulationsReader;

public class StockSimulationsCacheReader implements IStockSimulationsReader {
	
	private IgniteCache<StockSimulationReturnsKey, StockSimulationReturnsVal> stockSimulationsDataCache = null;
	private static final String PARTITIONED_CACHE_NAME = StockSimulationsCacheWriter.class.getSimpleName() + "Partitioned";
	private CacheConfiguration<StockSimulationReturnsKey, StockSimulationReturnsVal> stockSimulationsDataCacheCfg = new CacheConfiguration<>(PARTITIONED_CACHE_NAME);
	private JavaIgniteContext<String, String> jic;
	
	public void setJic(JavaIgniteContext<String, String> jic) {
		this.jic = jic;
	}

	private StockSimulationsCacheReader(){
		stockSimulationsDataCacheCfg.setBackups(1);
		stockSimulationsDataCacheCfg.setCacheMode(CacheMode.PARTITIONED);
		RendezvousAffinityFunction  affinityFunction = new RendezvousAffinityFunction ();
		stockSimulationsDataCacheCfg.setAffinity(affinityFunction);
		stockSimulationsDataCacheCfg.setIndexedTypes(StockSimulationReturnsKey.class,StockSimulationReturnsVal.class);
		stockSimulationsDataCache = Ignition.ignite("riskAnalyticsGrid").getOrCreateCache(stockSimulationsDataCacheCfg);
	
	}
	
	public StockSimulationsCacheReader(JavaIgniteContext<String, String> jic)
	{
		this();
		this.jic = jic;
	}


	@Override
	public DataFrame getStockSimulationReturnsDataFrame(LocalDate businessDate,String stock){
		/*
		 * If we use sql query , we are assuming that data is in cache.
		 * */
		String sql = "select simulationUUID,stockReturn from STOCKSIMULATIONRETURNSVAL where businessDate = ? and symbol = ?";
		DataFrame df =jic.fromCache(PARTITIONED_CACHE_NAME).sql(sql, businessDate,stock);
		return df;
	}
	
	@Override
	public DataFrame getStockSimulationReturnsDataFrame(LocalDate businessDate){
		String sql = "select simulationUUID,symbol,stockReturn from STOCKSIMULATIONRETURNSVAL where businessDate = ?";
		DataFrame df =jic.fromCache(PARTITIONED_CACHE_NAME).sql(sql, businessDate);
		return df;
	}
	
	@Override
	public JavaRDD<StockSimulation> getStockSimulationReturnsRDD(LocalDate businessDate){
		String sql = "select simulationUUID,symbol,stockReturn from STOCKSIMULATIONRETURNSVAL where businessDate = ?";
		DataFrame df =jic.fromCache(PARTITIONED_CACHE_NAME).sql(sql, businessDate);
		JavaRDD<StockSimulation> javaRDD = df.javaRDD().map(f-> new StockSimulation(f.getAs("SIMULATIONUUID"),f.getAs("SYMBOL"),f.getAs("STOCKRETURN")));
		return javaRDD;
	}
	
	public class StockSimulation implements Serializable{
		public StockSimulation(String simulationID, String symbol, Double stockReturn) {
			super();
			this.simulationID = simulationID;
			this.symbol = symbol;
			this.stockReturn = stockReturn;
		}
		private String simulationID;
		private String symbol;
		private Double stockReturn;
		public String getSimulationID() {
			return simulationID;
		}
		public void setSimulationID(String simulationID) {
			this.simulationID = simulationID;
		}
		public String getSymbol() {
			return symbol;
		}
		public void setSymbol(String symbol) {
			this.symbol = symbol;
		}
		public Double getStockReturn() {
			return stockReturn;
		}
		public void setStockReturn(Double stockReturn) {
			this.stockReturn = stockReturn;
		}
	}
	/*public List<StockSimulationReturnsVal> getStockSimulationReturns(Date businessDate,String stock){
		List<StockSimulationReturnsVal> stockSimulationList = new ArrayList<>();
		
		System.out.println("Getting simmulations for stock "+stock+" for business date "+businessDate);
		
		String sql = "businessDate = ? and symbol = ?";
		
		stockSimulationsDataCache.query(new SqlQuery<StockSimulationReturnsKey,StockSimulationReturnsVal>(StockSimulationReturnsVal.class,sql)
				.setArgs(businessDate,stock)).getAll().forEach(simulation -> stockSimulationList.add(simulation.getValue()));
		
		System.out.println("Simulations # "+stockSimulationList.size());
		return stockSimulationList;
	}*/
	
	
	

}
