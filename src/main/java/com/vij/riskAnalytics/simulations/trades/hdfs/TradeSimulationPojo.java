package com.vij.riskAnalytics.simulations.trades.hdfs;

import java.io.Serializable;

public class TradeSimulationPojo implements Serializable{
	public TradeSimulationPojo(String dealId, String simulationId, String underlyer, double simulatedPnLAmt,
			double position, String portfolio, int businessDate) {
		super();
		this.dealId = dealId;
		this.simulationId = simulationId;
		this.underlyer = underlyer;
		this.simulatedPnLAmt = simulatedPnLAmt;
		this.position = position;
		this.portfolio = portfolio;
		this.businessDate = businessDate;
	}
	@Override
	public String toString() {
		return "TradeSimulation [dealId=" + dealId + ", simulationId=" + simulationId + ", underlyer=" + underlyer
				+ ", simulatedPnLAmt=" + simulatedPnLAmt + ", position=" + position + ", portfolio=" + portfolio
				+ ", businessDate=" + businessDate + "]";
	}
	private String dealId;
	private String simulationId;
	private String underlyer;
	private double simulatedPnLAmt;
	private double position;
	private String portfolio;
	private int businessDate;
}

