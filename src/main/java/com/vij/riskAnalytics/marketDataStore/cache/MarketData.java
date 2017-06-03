package com.vij.riskAnalytics.marketDataStore.cache;

import java.io.Serializable;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class MarketData implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	@QuerySqlField
	private Double eodPrice;

	public MarketData(Double eodPrice) {
		super();
		this.eodPrice = eodPrice;
	}

	public Double getEodPrice() {
		return eodPrice;
	}
}
