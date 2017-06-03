package com.vij.riskAnalytics.simulations.trades.hdfs;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class TradeSimulationsHDFSReader implements Serializable {

	private SQLContext sqlContext = null;
	private String businessDateStr = null;
	public TradeSimulationsHDFSReader(LocalDate businessDate,JavaSparkContext sc, String fileName){
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
		businessDateStr = businessDate.format(formatter);
		sqlContext = new SQLContext(sc);
		DataFrame df = sqlContext.parquetFile(fileName);
		df.registerTempTable("TradeSimulations");
		df.cache();
	}
	public  JavaRDD<TradeSimulation> getTradeSimulationsRDD(String dealID,String book){
		String queryStr = "select * from TradeSimulations where businessDate ="+businessDateStr + " and book='"+ book+"' and dealId ='"+dealID+"'";
		DataFrame tradeSimulationsDF = sqlContext.sql(queryStr);
		//tradeSimulationsDF.printSchema();
		return tradeSimulationsDF.javaRDD().map(f-> convertToTradeSimulation(f));
		
	}
	
	private  TradeSimulation convertToTradeSimulation(Row row)
	{
		
		return TradeSimulation.newBuilder().setDealId(row.getAs("dealId"))
				.setSimulationId(row.getAs("simulationId"))
				.setUnderlyer(row.getAs("underlyer"))
				.setSimulatedPnLAmt(row.getAs("simulatedPnLAmt"))
				.setPosition(row.getAs("position"))
				.setPortfolio(row.getAs("portfolio"))
				.setBusinessDate(row.getAs("business_date"))
				.build();
	}
}
