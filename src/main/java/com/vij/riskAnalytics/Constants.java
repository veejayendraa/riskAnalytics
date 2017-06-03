package com.vij.riskAnalytics;

public interface Constants {
	
	public final String LOCAL_CLIENT_CACHE_CONFIG_XML = "config/risk-analytics-cache-client.xml";
	public final String LOCAL_SERVER_CACHE_CONFIG_XML = "config/risk-analytics-cache-server.xml";
	public final String AWS_CLIENT_CACHE_CONFIG_XML = "config/aws-cloud/risk-analytics-cache-client-aws.xml";
	public final String RISK_ANALYTICS_DATA_DIR_SERVER = "/home/hadoop/riskAnalytics/data/";
	public final String RISK_ANALYTICS_DATA_DIR_LOCAL = "./data/";
	public final Integer NO_OF_SIMULATIONS = 10000;
	public final Integer HISTORICAL_DAYS_NEEDED = 290;
	public final Integer DEFAULT_PARALLELISM = 100;
	public final String BASE_TRADE_SIMULATION_DIR = "D://tradeSimulations/parquet";
	
	
}
