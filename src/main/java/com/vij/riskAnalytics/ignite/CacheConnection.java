package com.vij.riskAnalytics.ignite;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;

import com.vij.riskAnalytics.Constants;


public class CacheConnection {
	private static final Logger log = Logger.getLogger(CacheConnection.class.getName());
	/**
	 * This should be the first method that needs to be called for using ignite data grid.
	 * */
	
	private static CacheConnection cacheConnection;
	
	private CacheConnection(){
		//init();
	}
	public static void startIgnition() {
		
		String env = System.getProperty("env", "aws");
		if(env.equals("aws")){
			log.log(Level.INFO, ">>> RUNNING IN AWS ENVIRONMENT <<<");
			 Ignition.start(Constants.AWS_CLIENT_CACHE_CONFIG_XML);
		}else{
			log.log(Level.INFO, ">>> RUNNING IN LOCAL ENVIRONMENT <<<");
			 Ignition.start(Constants.LOCAL_CLIENT_CACHE_CONFIG_XML);
		}
	}
	
	
	public void init()
	{
		String env = System.getProperty("env", "aws");
		if(env.equals("aws")){
			log.log(Level.INFO, ">>> RUNNING IN AWS ENVIRONMENT <<<");
			 Ignition.start(Constants.AWS_CLIENT_CACHE_CONFIG_XML);
		}else{
			log.log(Level.INFO, ">>> RUNNING IN LOCAL ENVIRONMENT <<<");
			 Ignition.start(Constants.LOCAL_CLIENT_CACHE_CONFIG_XML);
		}
		//log.log(Level.INFO,"Established Cache Connection...");
	}
	
	/**
	 * This should be the last method that needs to be called while using ignite data grid ,
	 *  otherwise client JVM will keep on running
	 * */
	public static void stopIgnition()
	{
		log.log(Level.INFO, "Going to close Cache Connection...");
		Ignition.stop("riskAnalyticsGrid",false);
		cacheConnection = null;
		log.log(Level.INFO, "Cache Connection Closed...");
	}
}
