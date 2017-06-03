package com.vij.riskAnalytics.ignite;

import org.apache.ignite.Ignition;

import com.vij.riskAnalytics.Constants;

public class StartNode {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.setProperty("IGNITE_H2_DEBUG_CONSOLE", "true");
		//System.setProperty("IGNITE_QUIET","false");
		//System.setProperty("IGNITE_PERFORMANCE_SUGGESTIONS_DISABLED","true");
		//Ignition.setClientMode(false);
		Ignition.start(Constants.LOCAL_SERVER_CACHE_CONFIG_XML);
		

	}

}
