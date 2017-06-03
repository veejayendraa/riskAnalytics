package com.vij.riskAnalytics.simulations.cache;

import java.io.Serializable;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class StockSimulationReturnsVal implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	@QuerySqlField
	private double stockReturn;

	public StockSimulationReturnsVal(double stockReturn) {
		super();
		this.stockReturn = stockReturn;
	}
	
	public double getStockReturn() {
		return stockReturn;
	}

}
