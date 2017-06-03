package com.vij.riskAnalytics.simulations.stocks.hdfs;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Locale;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import com.vij.riskAnalytics.simulations.IStockSimulationsReader;
import com.vij.riskAnalytics.simulations.cache.StockSimulationsCacheReader.StockSimulation;

public class StockSimulationsHDFSReader implements IStockSimulationsReader {

	
	private transient JavaSparkContext jsc;
	
	public StockSimulationsHDFSReader(JavaSparkContext jsc) {
		
		this.jsc = jsc;
		
	}
	
	public DataFrame readSimulationsAboveGivenDate(LocalDate businessDate ){
		//new SQLContext(jsc).read().parquet("D://junk/parquet/simulations/BUSINESSDATE").printSchema();
		//SimpleDateFormat dateTimeFormatter = new SimpleDateFormat("yyyyMMdd",Locale.ENGLISH);
		SQLContext sqlContext = new SQLContext(jsc);
		sqlContext.read().parquet("D://junk/parquet/simulations").registerTempTable("simulations");	
		return sqlContext.sql("select * from simulations where BUSINESSDATE > "+businessDate);
		
	}
	public DataFrame getStockSimulationReturnsDataFrame(LocalDate businessDate,String symbol ){
		//SimpleDateFormat dateTimeFormatter = new SimpleDateFormat("yyyyMMdd",Locale.ENGLISH);
		SQLContext sqlContext = new SQLContext(jsc);
		sqlContext.read().parquet("D://junk/parquet/simulations").registerTempTable("simulations");	
		return sqlContext.sql("select * from simulations where BUSINESSDATE ="+businessDate+" and SYMBOL ='"+symbol+"'");
		//jsc.close();
	}
	
	public DataFrame getStockSimulationReturnsDataFrame(LocalDate businessDate ){
		//SimpleDateFormat dateTimeFormatter = new SimpleDateFormat("yyyyMMdd",Locale.ENGLISH);
		SQLContext sqlContext = new SQLContext(jsc);
		sqlContext.read().parquet("D://junk/parquet/simulations").registerTempTable("simulations");	
		return sqlContext.sql("select * from simulations where BUSINESSDATE ="+businessDate);
		//jsc.close();
	}
	
	public static void main(String[] args) {
		 //SimpleDateFormat dateTimeFormatter = new SimpleDateFormat("yyyy-MM-dd",Locale.ENGLISH);
		 SparkConf conf = new SparkConf();
		 conf.setAppName("HDFS Simulation Reader");
		 conf.setMaster("local[3]");
		 conf.setExecutorEnv("spark.executor.memory", "1g");
		 StockSimulationsHDFSReader readSimulations = new StockSimulationsHDFSReader(new JavaSparkContext(conf));
		//readSimulations.getStockSimulationReturnsDataFrame(dateTimeFormatter.parse("2016-03-17"),"TFSC");
		readSimulations.getStockSimulationReturnsDataFrame(LocalDate.parse(("2016-03-17"),DateTimeFormatter.ofPattern("yyyy-MM-dd",Locale.ENGLISH)));

	}

	@Override
	public JavaRDD<StockSimulation> getStockSimulationReturnsRDD(LocalDate businessDate) {
		// TODO Auto-generated method stub
		return null;
	}

}
