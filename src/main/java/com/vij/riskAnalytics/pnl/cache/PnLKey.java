package com.vij.riskAnalytics.pnl.cache;

import java.io.Serializable;
import java.util.Date;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class PnLKey implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	@QuerySqlField(index = true,orderedGroups={@QuerySqlField.Group(
		    name = "date_tkr_idx", order = 0)})
	private Date businessDate;
	@QuerySqlField(orderedGroups={@QuerySqlField.Group(
		    name = "date_tkr_idx", order = 1)})
	private String ticker;
	
	public PnLKey(Date businessDate, String ticker) {
		super();
		this.businessDate = businessDate;
		this.ticker = ticker;
	}
	
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((businessDate == null) ? 0 : businessDate.hashCode());
		result = prime * result + ((ticker == null) ? 0 : ticker.hashCode());
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
		PnLKey other = (PnLKey) obj;
		if (businessDate == null) {
			if (other.businessDate != null)
				return false;
		} else if (!businessDate.equals(other.businessDate))
			return false;
		if (ticker == null) {
			if (other.ticker != null)
				return false;
		} else if (!ticker.equals(other.ticker))
			return false;
		return true;
	}
	public Date getBusinessDate() {
		return businessDate;
	}
	public String getTicker() {
		return ticker;
	}


	@Override
	public String toString() {
		return "PnLKey [businessDate=" + businessDate + ", ticker=" + ticker + "]";
	}


}
