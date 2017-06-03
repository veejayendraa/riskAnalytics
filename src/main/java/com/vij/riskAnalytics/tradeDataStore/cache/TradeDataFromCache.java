package com.vij.riskAnalytics.tradeDataStore.cache;

import java.io.Serializable;

public class TradeDataFromCache implements Serializable{
	
	private String portfolio;
	private String underlyer;
	private double netInvestment;
	private String dealId;
	
	public TradeDataFromCache(String portfolio, String underlyer, double netInvestment, String dealId) {
		super();
		this.portfolio = portfolio;
		this.underlyer = underlyer;
		this.netInvestment = netInvestment;
		this.dealId = dealId;
	}
	public String getPortfolio() {
		return portfolio;
	}
	public void setPortfolio(String portfolio) {
		this.portfolio = portfolio;
	}
	public String getUnderlyer() {
		return underlyer;
	}
	public void setUnderlyer(String underlyer) {
		this.underlyer = underlyer;
	}
	public double getNetInvestment() {
		return netInvestment;
	}
	public void setNetInvestment(double netInvestment) {
		this.netInvestment = netInvestment;
	}
	public String getDealId() {
		return dealId;
	}
	public void setDealId(String dealId) {
		this.dealId = dealId;
	}
} 