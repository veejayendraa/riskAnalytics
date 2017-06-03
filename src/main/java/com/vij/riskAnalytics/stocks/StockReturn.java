package com.vij.riskAnalytics.stocks;

import java.io.Serializable;

public class StockReturn implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public StockReturn(String symbol, Double[] dailyReturns) {
		super();
		this.symbol = symbol;
		this.dailyReturns = dailyReturns;
	}
	
	private String symbol;
	private Double[] dailyReturns;
	public String getSymbol() {
		return symbol;
	}
	public Double[] getDailyReturns() {
		return dailyReturns;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((symbol == null) ? 0 : symbol.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		StockReturn other = (StockReturn) obj;
		if (symbol == null) {
			if (other.symbol != null)
				return false;
		} else if (!symbol.equals(other.symbol))
			return false;
		return true;
	}

}
