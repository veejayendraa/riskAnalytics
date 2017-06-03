package com.vij.riskAnalytics.VaR.whatIf;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class WhatIfQuery {
	
	private LocalDate businessDate;
	private String portfolio;
	//private String stock;
	//private double investment;
	private Map<String,Double> stockInvestmentMap = new HashMap<String,Double>();
	
	public WhatIfQuery(String businessDate, String portfolio, Map<String,Double> stockInvestmentMap) {
		super();
		this.businessDate = LocalDate.parse(businessDate, DateTimeFormatter.ofPattern("yyyy-MM-dd",Locale.ENGLISH));;
		this.portfolio = portfolio;
		this.stockInvestmentMap = stockInvestmentMap;
		//this.stock = stock;
		//this.investment = Double.parseDouble(investment);
	}

	public LocalDate getBusinessDate() {
		return businessDate;
	}
	
	public String getPortfolio() {
		return portfolio;
	}

	public Map<String, Double> getStockInvestmentMap() {
		return stockInvestmentMap;
	}

	@Override
	public String toString() {
		return "WhatIfQuery [businessDate=" + businessDate + ", portfolio=" + portfolio + ", stockInvestmentMap="
				+ stockInvestmentMap + "]";
	}
	
	
	
}
