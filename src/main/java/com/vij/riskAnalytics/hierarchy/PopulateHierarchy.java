package com.vij.riskAnalytics.hierarchy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Collections2;
import com.vij.riskAnalytics.Constants;
import com.vij.riskAnalytics.hierarchy.csv.pojo.NodeCSVPOJO;
import com.vij.riskAnalytics.hierarchy.store.cache.HierarchyDataCacheStore;
import com.vij.riskAnalytics.ignite.CacheConnection;


public class PopulateHierarchy {

	private static final Logger log = Logger.getLogger(PopulateHierarchy.class.getName());
	public static void main(String args[])
	{
		String env = System.getProperty("env", "aws");
		String data_dir;
		if(env.equals("aws")){
			log.log(Level.INFO, ">>> RUNNING IN AWS ENVIRONMENT <<<");
			data_dir = Constants.RISK_ANALYTICS_DATA_DIR_SERVER;
		}else{
			log.log(Level.INFO, ">>> RUNNING IN LOCAL ENVIRONMENT <<<");
			data_dir = Constants.RISK_ANALYTICS_DATA_DIR_LOCAL;
		}
		
		/*
		 * args[0] - COB for which hierarchy needs to be loaded.
		 * */
		System.out.println(args);
		if(args.length == 0)
		{
			throw new RuntimeException("Specify COB for which Hierarchy needs to be loaded !!! ");
		}else{
			log.log(Level.INFO,"Loading hierachy for COB "+args[0]);
		}
		String fileName = data_dir+"hierarchy/"+args[0]+"/hierarchy.csv";
		
		List<String> lines;
		try {
			
			lines = Files.readAllLines(Paths.get(fileName));
			List<NodeCSVPOJO> nodeCSVPOJOList = new ArrayList<NodeCSVPOJO>();
			Map<Node, List<Node>> map= null;
			try (Stream<String> stream = Files.lines(Paths.get(fileName))) {
				 map=  stream.skip(1).map(line-> {
					   String[] cols = line.split(",");
					   return new NodeCSVPOJO(cols[1],cols[0]);
					   }).sorted( new Comparator<NodeCSVPOJO>()
					   					{
											@Override
											public int compare(NodeCSVPOJO arg0, NodeCSVPOJO arg1) {
												return arg0.getParentNode().compareTo(arg1.getParentNode());
											}
										}).collect(Collectors.groupingBy(csvNode-> new Node(csvNode.getParentNode(),args[0]),
												Collectors.mapping(csvNode->
												new Node(csvNode.getChildNode(),args[0]),
							                    Collectors.toList())));
						 /*.collect(Collectors.groupingBy(NodeCSVPOJO::getParentNode,Collectors.mapping(
												NodeCSVPOJO::getChildNode,
							                    Collectors.toList())));*/
				   }
						   
			
			System.out.println(givePortfolios(args[0],"ParisDesk",map));
			 CacheConnection.startIgnition();
				
			 
			HierarchyDataCacheStore.getInstance().putHierarchyDataInCache(map);
			//saveAsHierarchyInCache(map);
			System.out.println(HierarchyDataCacheStore.getInstance().givePortfolios(args[0],"ParisDesk"));
			CacheConnection.stopIgnition();
		} catch (IOException e1) {
			
			e1.printStackTrace();
		} 
		
	}
	
	
	private static List<String> givePortfolios(String businessDate,String node,Map<Node, List<Node>> map){
		
		List<String> portfolios = new ArrayList<String>();
		return givePortfolios(new Node(node,businessDate),map,portfolios);
	}
	
private static List<String> givePortfolios(Node node,Map<Node, List<Node>> map,List<String> portfolios){
		
		
		List<Node> childNodes = map.get(node);
		if(childNodes != null){
			for(Node childNode :childNodes){
				givePortfolios(childNode,map,portfolios);
			}
		}else{
			portfolios.add(node.getNode());
		}
		return portfolios;
	}
	 private static void saveAsHierarchyInCache(Map<Node, List<Node>> map){
		 
		 CacheConnection.startIgnition();
			
		 CacheConnection.stopIgnition();
	 }
	
}
