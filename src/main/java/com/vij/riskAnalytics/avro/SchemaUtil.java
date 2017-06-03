package com.vij.riskAnalytics.avro;

import org.kitesdk.data.DatasetDescriptor;

import com.vij.riskAnalytics.simulations.trades.hdfs.TradeSimulationPojo;



public class SchemaUtil {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		DatasetDescriptor rowDesc = new DatasetDescriptor.Builder()
			    .schema(TradeSimulationPojo.class)
			    .build();
		
		System.out.println(rowDesc.getSchema());
	}

}