package com.vij.riskAnalytics.pnl.cache;

import java.io.Serializable;

public class PnL implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private double eodPrice;
	private double pnl;
	
	public PnL(double eodPrice, double pnl) {
		super();
		this.eodPrice = eodPrice;
		this.pnl = pnl;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(eodPrice);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(pnl);
		result = prime * result + (int) (temp ^ (temp >>> 32));
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
		PnL other = (PnL) obj;
		if (Double.doubleToLongBits(eodPrice) != Double.doubleToLongBits(other.eodPrice))
			return false;
		if (Double.doubleToLongBits(pnl) != Double.doubleToLongBits(other.pnl))
			return false;
		return true;
	}
	public double getEodPrice() {
		return eodPrice;
	}
	public double getPnl() {
		return pnl;
	}
	@Override
	public String toString() {
		return "PnL [eodPrice=" + eodPrice + ", pnl=" + pnl + "]";
	}
	

}
