package com.vij.riskAnalytics.pnl.cache;

import java.util.Date;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;

public class PnLCacheManager {
	
	private PnLCacheManager(){
		Ignition.setClientMode(true);
		pnlCacheCfg.setBackups(1);
		pnlCacheCfg.setCacheMode(CacheMode.PARTITIONED);
		pnlCacheCfg.setIndexedTypes(PnLKey.class,PnL.class);    
		pnlCache = Ignition.start("config/risk-analytics-cache.xml").getOrCreateCache(pnlCacheCfg);
	}
	private static final String PARTITIONED_CACHE_NAME = PnLCacheManager.class.getSimpleName() + "Partitioned";
	private static CacheConfiguration<PnLKey, PnL> pnlCacheCfg = new CacheConfiguration<>(PARTITIONED_CACHE_NAME);
	private IgniteCache<PnLKey, PnL> pnlCache = null;
	private static PnLCacheManager cacheManager = new PnLCacheManager();
	public static PnLCacheManager getInstance()
	{
		return cacheManager;
	}
	
	public  void put(PnLKey pnlKeyOfTickerForCOB,PnL pnlOfTickerForCOB)
	{
		
		pnlCache.put(pnlKeyOfTickerForCOB, pnlOfTickerForCOB);
		
	}
	
	public  PnL get(PnLKey pnlKeyOfTickerForCOB)
	{
		System.out.println("Cache Size :: "+pnlCache.size(CachePeekMode.PRIMARY));
		return pnlCache.get(pnlKeyOfTickerForCOB);
	}
	/**
	 * This method will provides the PnL for a specific date ranges
	 * */
	public PnL[] getPnLsBetweenTwoCobsForTicker(String ticker,Date startDate,Date endDate)
	{
		 String sql = "ticker = ? and businessDate between ? and ?";
		 
		 System.out.println("PnL for ticker :"+ticker+" with COB between "+startDate.toString()+ "and "+endDate.toString() +"queried with SQL query): ");
				 return  pnlCache.query(new SqlQuery<PnLKey, PnL>(PnL.class, sql).
		                setArgs(ticker, endDate,startDate)).getAll().stream().map(obj-> {
		                	PnL pnl = obj.getValue();
		                	return pnl;
		                }).toArray(PnL[]::new);
				  
	}
	 /**
     * Prints message and query results.
     *
     * @param msg Message to print before all objects are printed.
     * @param col Query results.
     */
    private static void print(String msg, Iterable<?> col) {
        print(msg);
        print(col);
    }

    /**
     * Prints message.
     *
     * @param msg Message to print before all objects are printed.
     */
    private static void print(String msg) {
        System.out.println();
        System.out.println(">>> " + msg);
    }

    /**
     * Prints query results.
     *
     * @param col Query results.
     */
    private static void print(Iterable<?> col) {
        for (Object next : col)
            System.out.println(">>>     " + next);
    }

}
