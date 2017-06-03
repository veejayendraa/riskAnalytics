package com.vij.riskAnalytics.VaR.hdfs.eodBatch;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Locale;

import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.functions;

import com.vij.riskAnalytics.ignite.CacheConnection;
import com.vij.riskAnalytics.simulations.StockSimulationsReaderFactory;
import com.vij.riskAnalytics.simulations.cache.StockSimulationsCacheWriter;
import com.vij.riskAnalytics.simulations.stocks.hdfs.StockSimulationsHDFSWriter;

public class EODBatch {

	private transient JavaSparkContext jsc;
	//private transient CacheConnection cacheConnection = new CacheConnection();
	
	private EODBatch(SparkConf conf) {
		this.jsc = new JavaSparkContext(conf);
		
	}
	public void start(LocalDate businessDate )
	{
			String businessDateStr = businessDate.format( DateTimeFormatter.ofPattern("yyyyMMdd",Locale.ENGLISH));
			DataFrame df = StockSimulationsReaderFactory.getStockSimulationStore(jsc, businessDate, businessDate).getStockSimulationReturnsDataFrame(businessDate);
			new StockSimulationsHDFSWriter(businessDateStr,jsc).write(df);
			System.out.println("I am done !!!");
			CacheConnection.stopIgnition();
	}
	
	public static void main(String args[])
	{
		 SparkConf conf = new SparkConf();
		 conf.setAppName("EOD Batch");
		 conf.setMaster("local[3]");
		 conf.setExecutorEnv("spark.executor.memory", "1g");
		 EODBatch eodBatch = new EODBatch(conf);
		// eodBatch.start(dateTimeFormatter.parse("2016-03-17"));
		eodBatch.start(LocalDate.parse(args[0], DateTimeFormatter.ofPattern("yyyy-MM-dd",Locale.ENGLISH)));
	}
}
