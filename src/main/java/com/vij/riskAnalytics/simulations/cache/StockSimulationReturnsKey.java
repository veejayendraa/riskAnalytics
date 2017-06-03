package com.vij.riskAnalytics.simulations.cache;

import java.io.Serializable;
import java.time.LocalDate;

import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class StockSimulationReturnsKey implements Serializable{
	
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
	
	@AffinityKeyMapped
	@QuerySqlField
	private String simulationUUID;
	
	public StockSimulationReturnsKey(LocalDate businessDate, String symbol, String simulationUUID) {
		super();
		this.businessDate = businessDate;
		this.symbol = symbol;
		this.simulationUUID = simulationUUID;
	}
	public LocalDate getBusinessDate() {
		return businessDate;
	}
	public String getSymbol() {
		return symbol;
	}
	public String getsimulationUUID() {
		return simulationUUID;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((businessDate == null) ? 0 : businessDate.hashCode());
		result = prime * result + ((simulationUUID == null) ? 0 : simulationUUID.hashCode());
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
		StockSimulationReturnsKey other = (StockSimulationReturnsKey) obj;
		if (businessDate == null) {
			if (other.businessDate != null)
				return false;
		} else if (!businessDate.equals(other.businessDate))
			return false;
		if (simulationUUID == null) {
			if (other.simulationUUID != null)
				return false;
		} else if (!simulationUUID.equals(other.simulationUUID))
			return false;
		if (symbol == null) {
			if (other.symbol != null)
				return false;
		} else if (!symbol.equals(other.symbol))
			return false;
		return true;
	}
	
	
}
