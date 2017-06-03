package com.vij.riskAnalytics.tradeDataStore.cache;

import java.io.Serializable;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class TradeData implements Serializable {


	@QuerySqlField(index = true,orderedGroups={@QuerySqlField.Group(
		    name = "date_portfolio_idx", order = 1)})
	private String portfolio;
	
	@QuerySqlField(index = true)
	private String underlyer;
	
	@QuerySqlField
	private double netInvestment;
	
	public TradeData(String portfolio, String underlyer, double netInvestment) {
		super();
		this.portfolio = portfolio;
		this.underlyer = underlyer;
		this.netInvestment = netInvestment;
	}


	
	public String getPortfolio() {
		return portfolio;
	}

	public String getUnderlyer() {
		return underlyer;
	}

	public double getNetInvestment() {
		return netInvestment;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((portfolio == null) ? 0 : portfolio.hashCode());
		result = prime * result + ((underlyer == null) ? 0 : underlyer.hashCode());
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
		TradeData other = (TradeData) obj;
		if (portfolio == null) {
			if (other.portfolio != null)
				return false;
		} else if (!portfolio.equals(other.portfolio))
			return false;
		if (underlyer == null) {
			if (other.underlyer != null)
				return false;
		} else if (!underlyer.equals(other.underlyer))
			return false;
		return true;
	}


	
}
