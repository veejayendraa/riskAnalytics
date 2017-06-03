package com.vij.riskAnalytics.simulations;

import java.io.Serializable;
import java.time.LocalDate;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;

import com.vij.riskAnalytics.simulations.cache.StockSimulationsCacheReader.StockSimulation;


public interface IStockSimulationsReader extends Serializable{
	public DataFrame getStockSimulationReturnsDataFrame(LocalDate businessDate,String stock);
	public DataFrame getStockSimulationReturnsDataFrame(LocalDate businessDate);
	public JavaRDD<StockSimulation> getStockSimulationReturnsRDD(LocalDate businessDate);
}
