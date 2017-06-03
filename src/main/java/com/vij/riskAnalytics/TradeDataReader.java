package com.vij.riskAnalytics;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.vij.riskAnalytics.ignite.CacheConnection;
import com.vij.riskAnalytics.tradeDataStore.cache.TradeData;
import com.vij.riskAnalytics.tradeDataStore.cache.TradeDataCacheStore;
import com.vij.riskAnalytics.tradeDataStore.cache.TradeDataFromCache;
import com.vij.riskAnalytics.tradeDataStore.cache.TradeDataKey;

public class TradeDataReader {
	
	private static final Logger log = Logger.getLogger(TradeDataReader.class.getName());
	private static String env = System.getProperty("env", "aws");
	private static String data_dir;
	static{
		if(env.equals("aws")){
			log.log(Level.INFO, ">>> RUNNING IN AWS ENVIRONMENT <<<");
			data_dir = Constants.RISK_ANALYTICS_DATA_DIR_SERVER;
		}else{
			log.log(Level.INFO, ">>> RUNNING IN LOCAL ENVIRONMENT <<<");
			data_dir = Constants.RISK_ANALYTICS_DATA_DIR_LOCAL;
		}
	}

	public static void main(String args [])
	{
		HashMap<TradeDataKey,TradeData> tradeDataMap = readTradeDataFromFile();
		CacheConnection.startIgnition();
		TradeDataCacheStore tradeDataCacheStore = new TradeDataCacheStore();
		tradeDataCacheStore.putTradeDataInCache(tradeDataMap);
		//List<TradeDataFromCache> tradeDataList = tradeDataCacheStore.getTradeData(LocalDate.parse("2016-03-17", DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ENGLISH)),"portfolio1");
		CacheConnection.stopIgnition();
	}

	public static HashMap<TradeDataKey,TradeData> readTradeDataFromFile()
	{
		String env = System.getProperty("env", "aws");
		String data_dir;
		if(env.equals("aws")){
			log.log(Level.INFO, ">>> RUNNING IN AWS ENVIRONMENT <<<");
			data_dir = Constants.RISK_ANALYTICS_DATA_DIR_SERVER;
		}else{
			log.log(Level.INFO, ">>> RUNNING IN LOCAL ENVIRONMENT <<<");
			data_dir = Constants.RISK_ANALYTICS_DATA_DIR_LOCAL;
		}
		
		String fileName = data_dir+"trades/trades.csv";
		HashMap<TradeDataKey,TradeData> tradeDataSet = new HashMap<TradeDataKey,TradeData>();
		
		List<String> lines;
		try {
			
			lines = Files.readAllLines(Paths.get(fileName));
			boolean firstLine = true;
			for(String line:lines)
			{
				if(firstLine)
				{
					firstLine = false;
					continue;
				}
					String[] cols = line.split(",");
					LocalDate businessDate = null;
					businessDate = LocalDate.parse(cols[0], DateTimeFormatter.ofPattern("yyyyMMdd",Locale.ENGLISH));
					String dealID = cols[1];
					String portfolio = cols[2];
					String stock = cols[3];
					Double investment = Double.parseDouble(cols[4]);
					tradeDataSet.put(new TradeDataKey(dealID,businessDate),new TradeData(portfolio,stock,investment));
			}
		} catch (IOException e1) {
			
			e1.printStackTrace();
		} 
		return tradeDataSet;
	}
	public static List<String> getStocksList() throws IOException
	{
		return Files.readAllLines(Paths.get(data_dir, "StocksList.txt"));
	}
}
