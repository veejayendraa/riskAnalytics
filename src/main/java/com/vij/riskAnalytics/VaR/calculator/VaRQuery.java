package com.vij.riskAnalytics.VaR.calculator;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.Date;

public class VaRQuery implements Serializable {

	String portfolioName;
	String traderName;
	LocalDate businessDate;
	String dealId;
	String stockName;
	String hierarchy;
	
	public VaRQuery(String portfolioName, String traderName, LocalDate businessDate, String dealId, String stockName, String hierarchy) {
		super();
		this.portfolioName = portfolioName;
		this.traderName = traderName;
		this.businessDate = businessDate;
		this.dealId = dealId;
		this.stockName = stockName;
		this.hierarchy = hierarchy;
	}
	
	public String getPortfolioName() {
		return portfolioName;
	}
	public String getTraderName() {
		return traderName;
	}
	public LocalDate getBusinessDate() {
		return businessDate;
	}
	public String getDealId() {
		return dealId;
	}
	public String getStockName() {
		return stockName;
	}

	public String getHierarchy() {
		return hierarchy;
	}

	
	@Override
	public String toString() {
		return "VaRQuery [portfolioName=" + portfolioName + ", traderName=" + traderName + ", businessDate="
				+ businessDate + ", dealId=" + dealId + ", stockName=" + stockName +",hierarchy"+hierarchy+ "]";
	}
	
}
