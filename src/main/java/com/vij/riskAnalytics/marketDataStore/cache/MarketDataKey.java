package com.vij.riskAnalytics.marketDataStore.cache;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.Date;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class MarketDataKey implements Serializable,Comparable <MarketDataKey>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	@QuerySqlField(orderedGroups={@QuerySqlField.Group(
		    name = "date_symbol_idx", order = 1)})
	private String symbol;
	@QuerySqlField(index = true,orderedGroups={@QuerySqlField.Group(
		    name = "date_symbol_idx", order = 0)})
	private LocalDate businessDate;
	
	public MarketDataKey(LocalDate businessDate,String symbol) {
		super();
		this.symbol = symbol;
		this.businessDate = businessDate;
	}
	public String getSymbol() {
		return symbol;
	}
	public LocalDate getBusinessDate() {
		return businessDate;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((businessDate == null) ? 0 : businessDate.hashCode());
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
		MarketDataKey other = (MarketDataKey) obj;
		if (businessDate == null) {
			if (other.businessDate != null)
				return false;
		} else if (!businessDate.equals(other.businessDate))
			return false;
		if (symbol == null) {
			if (other.symbol != null)
				return false;
		} else if (!symbol.equals(other.symbol))
			return false;
		return true;
	}
	
	@Override
	public int compareTo(MarketDataKey o) {
		 return (this.businessDate.compareTo(o.getBusinessDate()));
	}

}
