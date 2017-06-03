package com.vij.riskAnalytics.pnl;

import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import com.vij.riskAnalytics.pnl.cache.PnL;
import com.vij.riskAnalytics.pnl.cache.PnLCacheManager;
import com.vij.riskAnalytics.pnl.cache.PnLKey;

public class PnLManager {
	
	public static void main(String[] args)
	{
		SimpleDateFormat formatter = new SimpleDateFormat("dd-MMM-yyyy");
		try {
			PnLCacheManager.getInstance().put(new PnLKey(formatter.parse("02-Mar-2016"),"YHOO"), new PnL(31.09,1.12));
			PnLCacheManager.getInstance().put(new PnLKey(formatter.parse("03-Mar-2016"),"YHOO"), new PnL(32.09,2.12));
			PnLCacheManager.getInstance().put(new PnLKey(formatter.parse("03-Mar-2016"),"GOOG"), new PnL(132.09,12.12));
			System.out.println(PnLCacheManager.getInstance().get(new PnLKey(formatter.parse("03-Mar-2016"),"GOOG")));
			System.out.println(PnLCacheManager.getInstance().get(new PnLKey(formatter.parse("03-Mar-2016"),"YHOO")));
			PnL[] pnls = PnLCacheManager.getInstance().getPnLsBetweenTwoCobsForTicker("YHOO",formatter.parse("03-Mar-2016"),formatter.parse("02-Mar-2016"));
			for(PnL pnl : pnls)
			{
				System.out.println(pnl);
			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}


