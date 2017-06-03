package com.vij.riskAnalytics.simulations.trades.hdfs;

public class TradeSimulationKey {

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + date;
		result = prime * result + ((dealNum == null) ? 0 : dealNum.hashCode());
		result = prime * result + ((simId == null) ? 0 : simId.hashCode());
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
		TradeSimulationKey other = (TradeSimulationKey) obj;
		if (date != other.date)
			return false;
		if (dealNum == null) {
			if (other.dealNum != null)
				return false;
		} else if (!dealNum.equals(other.dealNum))
			return false;
		if (simId == null) {
			if (other.simId != null)
				return false;
		} else if (!simId.equals(other.simId))
			return false;
		return true;
	}
	public TradeSimulationKey(String simId, String dealNum, int date) {
		super();
		this.simId = simId;
		this.dealNum = dealNum;
		this.date = date;
	}
	private String simId;
	private String dealNum;
	private int date;
}
