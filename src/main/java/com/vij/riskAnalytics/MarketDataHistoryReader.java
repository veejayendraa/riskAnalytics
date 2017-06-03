package com.vij.riskAnalytics;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletionService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

import org.apache.commons.lang.time.DateUtils;

import com.vij.riskAnalytics.ignite.CacheConnection;
import com.vij.riskAnalytics.marketDataStore.HistoricalMarketDataStore;
import com.vij.riskAnalytics.marketDataStore.cache.MarketData;
import com.vij.riskAnalytics.marketDataStore.cache.MarketDataKey;



public class MarketDataHistoryReader  {
	
	private static final Logger log = Logger.getLogger(MarketDataHistoryReader.class.getName());
	private LocalDate startDate = null;
	private LocalDate endDate = null;
	private String data_dir = null;
	public MarketDataHistoryReader()
	{
		startDate = LocalDate.parse("2014-01-03",DateTimeFormatter.ofPattern("yyyy-MM-dd",Locale.ENGLISH));
		endDate = LocalDate.parse("2016-03-18",DateTimeFormatter.ofPattern("yyyy-MM-dd",Locale.ENGLISH));
		
		String env = System.getProperty("env", "aws");
		if(env.equals("aws")){
			log.log(Level.INFO, ">>> RUNNING IN AWS ENVIRONMENT <<<");
			data_dir = Constants.RISK_ANALYTICS_DATA_DIR_SERVER;
		}else{
			log.log(Level.INFO, ">>> RUNNING IN LOCAL ENVIRONMENT <<<");
			data_dir = Constants.RISK_ANALYTICS_DATA_DIR_LOCAL;
		}
	}
	public Double[] readYahooHistory(LocalDate businessDate,String symbol)
	{
		Double[] eodPrices = null;
		SimpleDateFormat dateTimeFormatter = new SimpleDateFormat("yyyymmdd",Locale.ENGLISH);
		String fileName = data_dir+"stocks/historical/"+dateTimeFormatter.format(businessDate)+"/"+symbol;
		
		try (Stream<String> stream = Files.lines(Paths.get(fileName))) {
			eodPrices =  (Double[]) stream.skip(0).map(line->{
				String[] cols = line.split(",");
				Double eodPrice = Double.parseDouble(cols[6]);
				return eodPrice;
			}).toArray();
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		
		return eodPrices;
	}
	
	/**
	 * A batch would run to read the EOD prices from a file.This file would come be written by separate EOD batch job
	 * which would read the data from Yahoo/Investment.com
	 * */
	public HashMap<String,Double> readYahooHistory(LocalDate businessDate)
	{
		HashMap<String,Double> eodPricesForSymbol = new HashMap<String,Double>();
		SimpleDateFormat dateTimeFormatter = new SimpleDateFormat("yyyymmdd",Locale.ENGLISH);
		String fileName = data_dir+"stocks/working/"+dateTimeFormatter.format(businessDate);
		try (Stream<String> stream = Files.lines(Paths.get(fileName))) {
			stream.skip(0).map(line->{
				String[] cols = line.split(",");
				Double eodPrice = Double.parseDouble(cols[6]);
				String symbol = cols[7];
				eodPricesForSymbol.put(symbol, eodPrice);
				return eodPricesForSymbol;
			});
			 
			 
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		
		return eodPricesForSymbol;
	}
	
	public Map<MarketDataKey,MarketData> readYahooHistory(String symbol,boolean isMarketData)
	{
		HashMap<MarketDataKey,MarketData> eodPricesForSymbol = new HashMap<MarketDataKey,MarketData>();
		
		String fileName = isMarketData ? data_dir+"marketFactors/historical/yahoo/"+symbol+".csv":data_dir+"stocks/historical/"+symbol+".csv";
		log.log(Level.INFO,"Running for file =  "+fileName);
		
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
					businessDate = LocalDate.parse(cols[0],DateTimeFormatter.ofPattern("yyyy-MM-dd",Locale.ENGLISH));
					Double eodPrice = Double.parseDouble(cols[6]);
					eodPricesForSymbol.put(new MarketDataKey(businessDate,symbol),new MarketData(eodPrice));
			}
		} catch (IOException e1) {
			
			e1.printStackTrace();
		} 
		
		return fillInHistories(eodPricesForSymbol);
	}
	
	private TreeMap<MarketDataKey,MarketData> fillInHistories(HashMap<MarketDataKey,MarketData> data)
	{
		TreeMap<MarketDataKey,MarketData> sortedData =  new TreeMap<MarketDataKey,MarketData>();
		
		MarketDataKey[] dataKeys = null;
		dataKeys = data.keySet().parallelStream()
				.sorted()
				.filter(key-> key.getBusinessDate().isAfter(startDate.minusDays(1))).filter(key-> isBusinessDay(key.getBusinessDate()))
				.toArray(MarketDataKey[]::new);
		
		LocalDate currentDate = startDate;
		int orignalDataCnt = 0;
		MarketDataKey backFillKey = null;
		while((currentDate.isBefore(endDate) || currentDate.equals(endDate)) && orignalDataCnt <= dataKeys.length-1)
		{
			/*if (!isBusinessDay(dataKeys[orignalDataCnt].getBusinessDate()))
			{
				currentDate = DateUtils.addDays(currentDate, 1); // skip sat/sun
				orignalDataCnt++;
				continue;
			}*/
			
			if(dataKeys[orignalDataCnt].getBusinessDate().equals(currentDate))
			{
				backFillKey = dataKeys[orignalDataCnt];
				//System.out.println("Putting data for "+dataKeys[orignalDataCnt].getBusinessDate()+" with price "+data.get(dataKeys[orignalDataCnt]).getEodPrice());
				sortedData.put(dataKeys[orignalDataCnt], data.get(dataKeys[orignalDataCnt]));
			}else{
				if(backFillKey == null)
				{
					backFillKey = dataKeys[orignalDataCnt];
				}
				backFill(currentDate,dataKeys[orignalDataCnt].getBusinessDate(),sortedData,dataKeys[orignalDataCnt],data.get(backFillKey));
				/* after doing back filling put the current date equal to the date till which back filling has been done.*/
				currentDate = dataKeys[orignalDataCnt].getBusinessDate();
				//System.out.println("Putting data for "+dataKeys[orignalDataCnt].getBusinessDate()+" with price "+data.get(dataKeys[orignalDataCnt]).getEodPrice());
				sortedData.put(dataKeys[orignalDataCnt], data.get(dataKeys[orignalDataCnt]));
			}
			
			currentDate = currentDate.plusDays(1);
			orignalDataCnt++;
		}
		
		if(currentDate.isBefore(endDate))
		{
			backFill(currentDate,endDate,sortedData,dataKeys[dataKeys.length-1],data.get(backFillKey));
			//System.out.println("Putting data for "+endDate+" with price "+data.get(backFillKey).getEodPrice());
			sortedData.put(new MarketDataKey(endDate,dataKeys[dataKeys.length-1].getSymbol()), data.get(backFillKey));	
		}
			
		return sortedData;
	}
	
	private void backFill(LocalDate startDate,LocalDate endDate,TreeMap<MarketDataKey,MarketData> sortedData,MarketDataKey keyToBeFilled,MarketData data)
	{
		LocalDate businessDateToBeInserted = LocalDate.from(startDate) ;
		while(businessDateToBeInserted.isBefore(endDate))
		{
			if( ! isBusinessDay(businessDateToBeInserted))
			{
				businessDateToBeInserted = businessDateToBeInserted.plusDays(1);
				continue;
			}
			//System.out.println("Back Filling for COB "+businessDateToBeInserted +" with price "+data.getEodPrice());
			sortedData.put(new MarketDataKey(businessDateToBeInserted,keyToBeFilled.getSymbol()), data);			
			businessDateToBeInserted = businessDateToBeInserted.plusDays(1);
		}
	}
	
	private boolean isBusinessDay(LocalDate date){
		if(date.getDayOfWeek().compareTo(DayOfWeek.SATURDAY) == 0 || date.getDayOfWeek().compareTo(DayOfWeek.SUNDAY) == 0){
			return false;
		}
		
		return true;
	}
	
	
		
	public Map<MarketDataKey,MarketData> readInvestmentDotComHistory(String symbol)
	{
		HashMap<MarketDataKey,MarketData> eodPricesForSymbol = new HashMap<MarketDataKey,MarketData>();
		String fileName = data_dir+"marketFactors/historical/investment.com/"+symbol+".tsv";
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
					String[] cols = line.split("\t");
					LocalDate businessDate = null;
					businessDate = LocalDate.parse(cols[0],DateTimeFormatter.ofPattern("d-MMM-yy",Locale.ENGLISH));
					
					Double eodPrice = Double.parseDouble(cols[1]);
					eodPricesForSymbol.put(new MarketDataKey(businessDate,symbol),new MarketData(eodPrice));
			}
		} catch (IOException e1) {
			
			e1.printStackTrace();
		} 
		
		return fillInHistories(eodPricesForSymbol);
	}
	
	
	
	
	 public static void main( String[] args ){
		 
		
		 CacheConnection.startIgnition();
		 
		 readAndPutReturns();
		 
		 HistoricalMarketDataStore historicalMarketDataStore =  new HistoricalMarketDataStore(); 
		 historicalMarketDataStore.dailyReturns("TFSC",
				LocalDate.parse(("2016-01-01"), DateTimeFormatter.ofPattern(("yyyy-MM-dd"),Locale.ENGLISH)),
				LocalDate.parse(("2016-03-01"), DateTimeFormatter.ofPattern(("yyyy-MM-dd"),Locale.ENGLISH)));
		historicalMarketDataStore.dailyReturns("us30yeartreasurybonds",
				LocalDate.parse(("2015-12-01"), DateTimeFormatter.ofPattern(("yyyy-MM-dd"),Locale.ENGLISH)),
				LocalDate.parse(("2016-03-01"), DateTimeFormatter.ofPattern(("yyyy-MM-dd"),Locale.ENGLISH)));
		
		CacheConnection.stopIgnition();
	 }
	 
	 private static void readAndPutReturns(){
		 
		 MarketDataHistoryReader dataHistoryReaderFromWeb = new MarketDataHistoryReader();
		 
			ExecutorService executor = createExecutor(Runtime.getRuntime().availableProcessors());
			CompletionService<Boolean> completionService = new ExecutorCompletionService<Boolean>(executor);
			 try {
				List<String> stockList = TradeDataReader.getStocksList();
				for(String stock : stockList)
				 {
					completionService.submit(new HistoricalMarketDataStore(dataHistoryReaderFromWeb.readYahooHistory(stock,false)),true);
	 
				 }
				 completionService.submit(new HistoricalMarketDataStore(dataHistoryReaderFromWeb.readYahooHistory("^GSPC",true)),true);
				 completionService.submit(new HistoricalMarketDataStore(dataHistoryReaderFromWeb.readYahooHistory("^IXIC",true)),true);
				 completionService.submit(new HistoricalMarketDataStore(dataHistoryReaderFromWeb.readInvestmentDotComHistory("crudeoil")),true);
				 completionService.submit(new HistoricalMarketDataStore(dataHistoryReaderFromWeb.readInvestmentDotComHistory("us30yeartreasurybonds")),true);
				 
				 for(int i =1; i <= (TradeDataReader.getStocksList().size()+4);i++ )
				 {
					 completionService.take(); 
					 log.log(Level.INFO,"Completed tasks #"+ i);
				 }
				 executor.shutdown();
				 log.log(Level.INFO,"Shutting down executor...");
				 
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			 

	 }
	 public static List<String> getMarketFactorList() throws IOException
		{
			String data_dir;
			String env = System.getProperty("env", "aws");
			if(env.equals("aws")){
				log.log(Level.INFO, ">>> RUNNING IN AWS ENVIRONMENT <<<");
				data_dir = Constants.RISK_ANALYTICS_DATA_DIR_SERVER;
			}else{
				log.log(Level.INFO, ">>> RUNNING IN LOCAL ENVIRONMENT <<<");
				data_dir = Constants.RISK_ANALYTICS_DATA_DIR_LOCAL;
			}
			return Files.readAllLines(Paths.get(data_dir, "MarketFactorsList.txt"));
		}
	
	private static ExecutorService createExecutor(int threads){
		 int effectiveThreads = (threads <= 0 ? Runtime.getRuntime().availableProcessors() : threads);
		    ThreadFactory threadFactory = r -> {
		      Thread t = Executors.defaultThreadFactory().newThread(r);
		      t.setName("HistoryLoaderTaskRunner-" + t.getName());
		      t.setDaemon(true);
		      return t;
		    };
		    return Executors.newFixedThreadPool(effectiveThreads, threadFactory);
	 }
	
}
