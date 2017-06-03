package com.vij.riskAnalytics.VaR.calculator;

import java.io.Serializable;
import java.util.List;

public class VaRResult implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	@Override
	public String toString() {
		return "VaRResult [var=" + var + "]";
	}
	private double var;
	private List<Double> simulations;
	public VaRResult(double var, List<Double> simulations) {
		super();
		this.var = var;
		this.simulations = simulations;
	}
	public double getVar() {
		return var;
	}
	public void setVar(double var) {
		this.var = var;
	}
	public List<Double> getSimulations() {
		return simulations;
	}
	public void setSimulations(List<Double> simulations) {
		this.simulations = simulations;
	}
	
}
