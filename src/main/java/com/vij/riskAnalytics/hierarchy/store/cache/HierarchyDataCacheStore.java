package com.vij.riskAnalytics.hierarchy.store.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;

import com.vij.riskAnalytics.hierarchy.Node;

public class HierarchyDataCacheStore {

	private CacheConfiguration<Node, List<Node>> hierarchyDataCacheCfg = null;
	private HierarchyDataCacheStore(){
		hierarchyDataCacheCfg = new CacheConfiguration<>(PARTITIONED_CACHE_NAME);
		hierarchyDataCacheCfg.setBackups(1);
		hierarchyDataCacheCfg.setCacheMode(CacheMode.PARTITIONED);
		//hierarchyDataCacheCfg.setIndexedTypes(MarketDataKey.class,MarketData.class);
		Ignite ignite = Ignition.ignite("riskAnalyticsGrid");
		hierarchyDataCache = ignite.getOrCreateCache(hierarchyDataCacheCfg);
	}
	
	private IgniteCache<Node, List<Node>> hierarchyDataCache = null;
	private static HierarchyDataCacheStore cacheStore = new HierarchyDataCacheStore();
	
	public static HierarchyDataCacheStore getInstance(){
		return cacheStore;
	}
	private static final String PARTITIONED_CACHE_NAME = "HierarchyDataCache";
	
	public void putHierarchyDataInCache(Map<Node, List<Node>> hierarchyDataMap){
		hierarchyDataCache.putAll(hierarchyDataMap);
	}
	
	public List<String> givePortfolios(String businessDate,String node){
		
		List<String> portfolios = new ArrayList<String>();
		return givePortfolios(new Node(node,businessDate),portfolios);
	}
	
	private List<String> givePortfolios(Node node,List<String> portfolios){
		
		List<Node> childNodes = hierarchyDataCache.get(node);
		if(childNodes != null){
			for(Node childNode :childNodes){
				givePortfolios(childNode,portfolios);
			}
		}else{
			portfolios.add(node.getNode());
		}
		return portfolios;
	}
	
}
