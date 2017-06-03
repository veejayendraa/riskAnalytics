package com.vij.riskAnalytics;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.time.LocalDate;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.spark.IgniteContext;
import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import com.vij.riskAnalytics.simulations.cache.StockSimulationsCacheReader.StockSimulation;

import scala.collection.Seq;


public class IgniteCacheReaderWriter<K,V> implements Serializable {
	
	private IgniteContext<K, V> ic;
	private JavaIgniteContext<String, String> jic;
	private static final Logger log = Logger.getLogger(IgniteCacheReaderWriter.class.getName());
	//private IgniteCache<K, V> dataCache = null;
	private final String PARTITIONED_CACHE_NAME;
	private CacheConfiguration<K, V> cacheCfg = null;
	
	public IgniteCacheReaderWriter(JavaSparkContext jsc,Class<K> keyType,Class<V> valueType,String cacheName){
		PARTITIONED_CACHE_NAME = cacheName + "Partitioned";
		cacheCfg = new CacheConfiguration<>(PARTITIONED_CACHE_NAME);
		cacheCfg.setBackups(1);
		cacheCfg.setCacheMode(CacheMode.PARTITIONED);
		cacheCfg.setIndexedTypes(keyType,valueType);
		cacheCfg.setName(PARTITIONED_CACHE_NAME);
		RendezvousAffinityFunction  affinityFunction = new RendezvousAffinityFunction ();
		cacheCfg.setAffinity(affinityFunction);
		Ignition.ignite("riskAnalyticsGrid").getOrCreateCache(cacheCfg);
		
		String config = null;
		String env = System.getProperty("env", "aws");
		if(env.equals("aws")){
			log.log(Level.INFO, ">>> RUNNING IN AWS ENVIRONMENT <<<");
			  config = Constants.AWS_CLIENT_CACHE_CONFIG_XML;
		}else{
			log.log(Level.INFO, ">>> RUNNING IN LOCAL ENVIRONMENT <<<");
			config = Constants.LOCAL_CLIENT_CACHE_CONFIG_XML;
		}
		/*
		* When creating an IgniteContext instance, an optional boolean client argument (defaulting to true)
		* can be passed to context constructor.This is typically used in a Shared Deployment installation.
		* When client is set to false, context will operate in embedded mode and will start server nodes on all workers during the context construction.
		*/
		this.ic = new IgniteContext<K,V>(jsc.sc(), config,true);
		this.jic = new JavaIgniteContext<>(jsc, config);
	}
	public void put(JavaPairRDD<K,V> rdd){
		ic.fromCache(cacheCfg).savePairs(rdd.rdd(),true);
	}
	
	public <T> JavaRDD<T> get(Class<T> type,String businessDate,String book,String dealId){
		Field[] fields = type.getFields();
		StringBuffer sqlFields = new StringBuffer();
		int i = 1;
		for(Field field : fields){
			
			if(field.getName().equals("SCHEMA$"))
			{
				i++;
				continue;
			}
			if(i<fields.length)
			{
				if(field.getName().equals("portfolio"))
				{
					sqlFields.append("book").append(",");
				}else
				{
					sqlFields.append(field.getName()).append(",");
				}
				i++;
			}else{
				sqlFields.append(field.getName());
			}
			
		}
		
		String sql = "select "+sqlFields.toString()+ " from "+type.getSimpleName() +" where businessDate = ?  and book= ? and dealId = ? ";
		//System.out.println(sql);
		DataFrame df =jic.fromCache(PARTITIONED_CACHE_NAME).sql(sql, businessDate,book,dealId);
		JavaRDD<T> javaRDD = df.javaRDD().map(f->convert(f, type));
		return javaRDD;
	}
	private <T> T convert(Row row,Class<T> type)
	{
		Field[] fields = type.getFields();
		Class[] fieldTypes = new Class[fields.length-1];
		Object[] objects = new Object[fields.length-1];
		int i = 0;
		for(Field field: fields)
		{
			if(field.getName().equals("SCHEMA$"))
			{
				
				continue;
			}
			
			fieldTypes[i] = field.getType();
			if(field.getName().equals("portfolio"))
			{
				objects[i] = row.getAs("BOOK");
			}
			else
			{
			objects[i] = row.getAs(field.getName().toUpperCase());
			}
			i++;
		}
		Constructor constructor ;
		try {
			 constructor =
					type.getConstructor(fieldTypes);
			T instance = (T)constructor.newInstance(objects);
			return instance;
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
		/*MyObject myObject = (MyObject)
		        constructor.newInstance("constructor-arg1");
		return new StockSimulation(f.getAs("SIMULATIONUUID"),f.getAs("SYMBOL"),f.getAs("STOCKRETURN"));*/
	}
	
}
