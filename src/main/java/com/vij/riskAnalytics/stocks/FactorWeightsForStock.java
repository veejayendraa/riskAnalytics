package com.vij.riskAnalytics.stocks;

import java.io.Serializable;

public class FactorWeightsForStock implements Serializable {

	private String symbol;
	private double[] factorWeights;
	
	public FactorWeightsForStock(String symbol, double[] factorWeights) {
		super();
		this.symbol = symbol;
		this.factorWeights = factorWeights;
	}
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public String getSymbol() {
		return symbol;
	}
	public double[] getFactorWeights() {
		return factorWeights;
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
		FactorWeightsForStock other = (FactorWeightsForStock) obj;
		if (symbol == null) {
			if (other.symbol != null)
				return false;
		} else if (!symbol.equals(other.symbol))
			return false;
		return true;
	}

}
