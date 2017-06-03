package com.vij.riskAnalytics.tradeDataStore.cache;

import java.io.Serializable;
import java.time.LocalDate;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class TradeDataKey implements Serializable{
	
	@QuerySqlField
	private String dealId;
	@QuerySqlField(index = true,orderedGroups={@QuerySqlField.Group(
		    name = "date_portfolio_idx", order = 0)})
	private LocalDate businessDate;
	
	
	public TradeDataKey(String dealId, LocalDate businessDate) {
		super();
		this.dealId = dealId;
		this.businessDate = businessDate;
	}
	public String getDealId() {
		return dealId;
	}
	public LocalDate getBusinessDate() {
		return businessDate;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((businessDate == null) ? 0 : businessDate.hashCode());
		result = prime * result + ((dealId == null) ? 0 : dealId.hashCode());
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
		TradeDataKey other = (TradeDataKey) obj;
		if (businessDate == null) {
			if (other.businessDate != null)
				return false;
		} else if (!businessDate.equals(other.businessDate))
			return false;
		if (dealId == null) {
			if (other.dealId != null)
				return false;
		} else if (!dealId.equals(other.dealId))
			return false;
		return true;
	}


}
