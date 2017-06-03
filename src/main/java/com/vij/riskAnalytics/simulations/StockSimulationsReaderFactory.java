package com.vij.riskAnalytics.simulations;

import java.time.LocalDate;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.spark.api.java.JavaSparkContext;

import com.vij.riskAnalytics.Constants;
import com.vij.riskAnalytics.simulations.cache.StockSimulationsCacheReader;
import com.vij.riskAnalytics.simulations.stocks.hdfs.StockSimulationsHDFSReader;

public class StockSimulationsReaderFactory {

	private static final Logger log = Logger.getLogger(StockSimulationsReaderFactory.class.getName());

	public static IStockSimulationsReader getStockSimulationStore(JavaSparkContext jsc,LocalDate queryDate,LocalDate currentBusinessDate)
	{
		
		/*
		 * Ideally currentBusinessDate should be passed but if not take system date.
		 * 
		 */
		if(currentBusinessDate == null)
		{
			currentBusinessDate =  LocalDate.now();
			log.log(Level.INFO, "currentBusinessDate is null so using current system date as default date -> "+currentBusinessDate);

		}
		
		log.log(Level.INFO,"Check if query date is not 2 week old then get simulations form cache otherwise from HDFS");
		log.log(Level.INFO, "Query date = "+queryDate +" and Business date = "+currentBusinessDate);
		
		if(queryDate.plusDays(14).isAfter(currentBusinessDate))
		{
			log.log(Level.INFO, "***********GETTING FROM CACHE *********");
			
			if(System.getProperty("env","aws").equals("aws"))
			{
				log.log(Level.INFO, ">>> RUNNING IN AWS ENVIRONMENT <<<");
				return new StockSimulationsCacheReader(new JavaIgniteContext<>(jsc, Constants.AWS_CLIENT_CACHE_CONFIG_XML));
			}
			else
			{
				log.log(Level.INFO, ">>> RUNNING IN LOCAL ENVIRONMENT <<<");
				return new StockSimulationsCacheReader(new JavaIgniteContext<>(jsc, Constants.LOCAL_CLIENT_CACHE_CONFIG_XML));
			}
		}else{
			log.log(Level.INFO, "***********GETTING FROM HDFS*********");
			return new StockSimulationsHDFSReader(jsc);
		}
	}
}
