package com.vij.riskAnalytics.marketDataStore;

import java.time.LocalDate;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.Callable;

import com.vij.riskAnalytics.marketDataStore.cache.MarketData;
import com.vij.riskAnalytics.marketDataStore.cache.MarketDataCacheStore;
import com.vij.riskAnalytics.marketDataStore.cache.MarketDataKey;

public class HistoricalMarketDataStore implements Runnable{ 

	Map<MarketDataKey,MarketData> marketDataMap = null;
	public HistoricalMarketDataStore()
	{
		
	}
	
	public HistoricalMarketDataStore(Map<MarketDataKey,MarketData> marketDataMap)
	{
		this.marketDataMap = marketDataMap;
	}
	
	public Double[] dailyReturns(String symbol,LocalDate startDate,LocalDate endDate )
	{
		Double[] dailyPrices = MarketDataCacheStore.getInstance().getMktData(symbol, startDate, endDate);
		Double[] dailyReturns = new Double[dailyPrices.length - 1];
		Double prev = 0.0;
		Double current = 0.0;
		int idx = 0;
		for(Double price : dailyPrices)
		{
			if(idx == 0)
			{
				prev = price;
			}else
			{
				current = price;
				dailyReturns[idx-1]=(current - prev)/prev;
				prev = current;
			}
			idx++;
		}
		
		return dailyReturns;
	}
	
	public void persistMktData(Map<MarketDataKey,MarketData> marketDataMap)
	{
		//TODO at the same time write to some file as well and later on put the whole file in hdfs.
		
		MarketDataCacheStore.getInstance().putMktDataInCache(marketDataMap);
		
	}
	
	public void persistMktData(MarketDataKey marketDataKey,MarketData marketData)
	{
		//TODO at the same time write to some file as well and later on put the whole file in hdfs.
		MarketDataCacheStore.getInstance().putMktDataInCache(marketDataKey,marketData);
	}
	
	public void persistMktData(LocalDate businessDate,String symbol,Double eodPrice)
	{
		//TODO at the same time write to some file as well and later on put the whole file in hdfs.
		MarketDataCacheStore.getInstance().putMktDataInCache(new MarketDataKey(businessDate,symbol),new MarketData(eodPrice));
	}


	@Override
	public void run () {
		persistMktData(this.marketDataMap);
	}

}
