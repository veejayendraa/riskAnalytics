package com.vij.riskAnalytics.VaR.calculator;

import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.math3.distribution.MultivariateNormalDistribution;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealMatrixFormat;
import org.apache.commons.math3.random.MersenneTwister;
import org.apache.commons.math3.stat.correlation.Covariance;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;

import com.vij.riskAnalytics.Constants;
import com.vij.riskAnalytics.MarketDataHistoryReader;
import com.vij.riskAnalytics.TradeDataReader;
import com.vij.riskAnalytics.ignite.CacheConnection;
import com.vij.riskAnalytics.marketDataStore.HistoricalMarketDataStore;
import com.vij.riskAnalytics.simulations.StockSimulationsReaderFactory;
import com.vij.riskAnalytics.simulations.cache.StockSimulationReturnsKey;
import com.vij.riskAnalytics.simulations.cache.StockSimulationReturnsVal;
import com.vij.riskAnalytics.simulations.cache.StockSimulationsCacheReader;
import com.vij.riskAnalytics.simulations.cache.StockSimulationsCacheWriter;
import com.vij.riskAnalytics.stocks.FactorWeightsForStock;
import com.vij.riskAnalytics.stocks.StockReturn;
import com.vij.riskAnalytics.tradeDataStore.cache.TradeData;
import com.vij.riskAnalytics.tradeDataStore.cache.TradeDataCacheStore;

import scala.Tuple2;



public class ReturnSimulator implements Serializable {

	private transient JavaSparkContext jsc;
	private int historicalDaysNeeded = Constants.HISTORICAL_DAYS_NEEDED;
	private transient HistoricalMarketDataStore historicalMarketDataStore = new HistoricalMarketDataStore();
	private static int parallelism = Constants.DEFAULT_PARALLELISM; 
	private int numOfTrials = Constants.NO_OF_SIMULATIONS; 
	private long baseSeed = 1000L;
	transient private String data_dir = null;
	private static final Logger log = Logger.getLogger(ReturnSimulator.class.getName());

	public ReturnSimulator(){}
	private ReturnSimulator(SparkConf conf) {
		this.jsc = new JavaSparkContext(conf);
		String env = System.getProperty("env", "aws");
		if(env.equals("aws")){
			log.log(Level.INFO, ">>> RUNNING IN AWS ENVIRONMENT <<<");
			data_dir = Constants.RISK_ANALYTICS_DATA_DIR_SERVER;
		}else{
			log.log(Level.INFO, ">>> RUNNING IN LOCAL ENVIRONMENT <<<");
			data_dir = Constants.RISK_ANALYTICS_DATA_DIR_LOCAL;
		}
		
	}
	
	public void start(LocalDate businessDate){
		
		 long startTime = System.currentTimeMillis();
		 CacheConnection.startIgnition();
		
		/*each element of this list is array corresponding to daily returns of 1 stock.
		 We are considering 3 stocks so we have size as 3.
		*/
		List<StockReturn> stocksDailyReturn = getStocksReturn(businessDate);
		
		/*each element of this list is array corresponding to daily returns of 1 factor.
		  We are considering 4 factors so we have size as 4.
		 * */
		List<Double[]> factorsDailyReturn = getFactorsReturn(businessDate);
		
		/* convert this List into 2D array.*/
		
		double [][] factorDailyReturnArr = getArrFromList(factorsDailyReturn);
		
		/*This would be a 4*4 matrix as it contains correlation of every factor return.
		 * We are considering 21 days of data.*/ 
		
		double[][] factorCovariance = new Covariance(factorDailyReturnArr).getCovarianceMatrix().getData();
		
		/* each element of this array contains the mean of individual factor returns.
			So it has size of 4 as there are 4 factors.
		*/
		
		Double[] factorMeanTmp = factorsDailyReturn.parallelStream().map(eachFactorDailyReturn->sum(eachFactorDailyReturn)/eachFactorDailyReturn.length).toArray(Double[]:: new);
		double[] factorMean = ArrayUtils.toPrimitive(factorMeanTmp);
		/* add non linear behaviour */
		double [][] factorDailyReturnArrWithNonLinearity = new double[factorDailyReturnArr.length][];
		for(int i=0;i<factorDailyReturnArrWithNonLinearity.length;i++)
		{
			factorDailyReturnArrWithNonLinearity[i] = giveSomeNonLinearBehaviour(factorDailyReturnArr[i]);
		}
		
		/* this will give the weight to each factor (4 features and their square and square roots) for each stock.
		 So it will create a 3x12 matrix (currently we have 3 instruments) */
		
		List<FactorWeightsForStock> factorWeightsForAllStocks = computeFactorWeightsForAllStocks(stocksDailyReturn,factorDailyReturnArrWithNonLinearity);
		
		startSimulations(businessDate,factorWeightsForAllStocks,factorCovariance,factorMean);
		
		log.log(Level.INFO,"I am done - Simulations completed in time (ms) # "+ (System.currentTimeMillis() - startTime));
		
		 CacheConnection.stopIgnition();

		 		
	}
	
	
	private void startSimulations(LocalDate businessDate,List<FactorWeightsForStock> factorWeightsForAllStocks,double[][] factorCovariance,double[] factorMean)
	{
		// if factorWeightsForAllStocks is of large size then we have to change our model.
		Broadcast<List<FactorWeightsForStock>> factorWeightsBroadCastVariable =  jsc.broadcast(factorWeightsForAllStocks);
		List<Long> seedsList = new ArrayList<Long>(parallelism); 
		/* generate different base seeds for every trial */
		for(int i=0;i<parallelism;i++)
		{
			seedsList.add((long) (i + parallelism));
		}
		JavaRDD<Long> seedsRDD = jsc.parallelize(seedsList,parallelism);
		log.log(Level.INFO,"running with parallelism of # "+parallelism);
		int noOfTask = numOfTrials/parallelism;
		log.log(Level.INFO,"Number of task in each parallel run # "+noOfTask);
		JavaPairRDD<StockSimulationReturnsKey,StockSimulationReturnsVal> rdd = seedsRDD
				.map(
						seed->(trialReturn(businessDate,seed,factorWeightsBroadCastVariable.getValue(),factorCovariance,factorMean,noOfTask)))
				.flatMap(new IterratorNew()).mapToPair(f->new Tuple2<StockSimulationReturnsKey,StockSimulationReturnsVal>(f._1,f._2));
		
		saveToCache(rdd);
		
		
		
		//System.out.println("Simulations done  :: "+countSimulationsForCOB(businessDate));
	}
	
	
	class IterratorNew implements  FlatMapFunction<Map<StockSimulationReturnsKey, StockSimulationReturnsVal>, Tuple2<StockSimulationReturnsKey, StockSimulationReturnsVal>>{

		
		@Override
		public Iterable<Tuple2<StockSimulationReturnsKey, StockSimulationReturnsVal>> call(
				Map<StockSimulationReturnsKey, StockSimulationReturnsVal> t) throws Exception {
			
		List<Tuple2<StockSimulationReturnsKey, StockSimulationReturnsVal>> list =	new ArrayList<Tuple2<StockSimulationReturnsKey, StockSimulationReturnsVal>>();
			
			for(Entry<StockSimulationReturnsKey, StockSimulationReturnsVal> entry: t.entrySet())
			{
				list.add(new Tuple2<>(entry.getKey(),entry.getValue()));
			}
			
			return list;
		}

		
		
	}
	
	
	private void saveToCache(JavaPairRDD<StockSimulationReturnsKey,StockSimulationReturnsVal> rdd){
		/*CacheConnection.startIgnition();*/
		StockSimulationsCacheWriter  simulationsCacheStore = new StockSimulationsCacheWriter(jsc);
		simulationsCacheStore.putMktDataInCache(rdd);
		/*CacheConnection.stopIgnition();
		return 1;*/
		}
	private Map<StockSimulationReturnsKey, StockSimulationReturnsVal> trialReturn(LocalDate businessDate,Long seed,List<FactorWeightsForStock> factorWeightsForAllStocks,double[][] factorCovariance,double[] factorMean,int noOfTrials)
	{
		
		
		/* MersenneTwister is just a random number generator.*/
		MersenneTwister randomNumber = new MersenneTwister(seed);
				
		/*  as there could be relationship between each factor so we are using MultivariateNormalDistribution */
		MultivariateNormalDistribution multivariateNormalDistribution = new MultivariateNormalDistribution(randomNumber,factorMean,factorCovariance);
		Map<StockSimulationReturnsKey, StockSimulationReturnsVal> stockSimulationsMap = new HashMap<>();
		/*StockSimulationsCacheWriter  simulationsCacheStore = new StockSimulationsCacheWriter();*/
		
		log.log(Level.INFO,"********* No of Trials ************"+noOfTrials);
		
		for (int i=0;i<noOfTrials;i++) 
		{
			
			String uuid = UUID.randomUUID().toString();
			// sampling factor returns.
			double[] simulatedFactorReturnsWithNonLinearBehaviour = simulateFactorReturns(multivariateNormalDistribution);
		
			for(FactorWeightsForStock  factorWeightsForSingleStock: factorWeightsForAllStocks)
			{
				double stockReturn = instrumentTrialReturn(simulatedFactorReturnsWithNonLinearBehaviour,factorWeightsForSingleStock.getFactorWeights());
				stockSimulationsMap.put(new StockSimulationReturnsKey(businessDate,factorWeightsForSingleStock.getSymbol(),uuid), new StockSimulationReturnsVal(stockReturn));
			}
			
		}
		/*simulationsCacheStore.putMktDataInCache(stockSimulationsMap);*/
		return stockSimulationsMap;
		
	}
	
	/*calculate the return of particular stock under particular simulated factor return */
	
	private double instrumentTrialReturn(double[] simulatedFactorReturnsWithNonLinearBehaviour, double[] factorWeightsForSingleStock)
	{
		double instrumentTrialReturn = factorWeightsForSingleStock[0];
		int i=0;
		while(i<simulatedFactorReturnsWithNonLinearBehaviour.length)
		{
			instrumentTrialReturn+=simulatedFactorReturnsWithNonLinearBehaviour[i]*factorWeightsForSingleStock[i+1];
			i++;
		}
		return instrumentTrialReturn;
	}
	
	/* simulate and sample FactorReturns using Normal Distribution */
	
	private double[] simulateFactorReturns(MultivariateNormalDistribution multivariateNormalDistribution)
	{
		double[] simulatedFactorReturn = multivariateNormalDistribution.sample();
		// ???? Need to check as this is simulatedFactorReturn of all factors.
		double[] simulatedFactorReturnWithNonLinearBehaviour = giveSomeNonLinearBehaviour(simulatedFactorReturn);
		return simulatedFactorReturnWithNonLinearBehaviour;
	}
	/* this will give the weight to each factor (4 features and their square and square roots) for each stock.
	  So it will create a 3x12 matrix (currently we have 3 instruments) */
	private List<FactorWeightsForStock> computeFactorWeightsForAllStocks(List<StockReturn> stocksDailyReturn,double [][] factorDailyReturnArrWithNonLinearity)
	{
		OLSMultipleLinearRegression  linearRegressions = new OLSMultipleLinearRegression();
		List<FactorWeightsForStock> factorWeightsForAllStocks = new ArrayList<>(stocksDailyReturn.size());
		for(StockReturn dailyReturnFor1Stock : stocksDailyReturn)
		{
			linearRegressions =linearModel(ArrayUtils.toPrimitive(dailyReturnFor1Stock.getDailyReturns()),factorDailyReturnArrWithNonLinearity);
			factorWeightsForAllStocks.add(new FactorWeightsForStock(dailyReturnFor1Stock.getSymbol(),linearRegressions.estimateRegressionParameters()));
		}
		
		return factorWeightsForAllStocks;
	}
	
	private OLSMultipleLinearRegression linearModel(double [] dailyStockReturnFor1Stock,double [][] factorDailyReturnArrWithNonLinearity)
	{
		OLSMultipleLinearRegression olsMultipleLinearRegression = new OLSMultipleLinearRegression();
		olsMultipleLinearRegression.newSampleData(dailyStockReturnFor1Stock, factorDailyReturnArrWithNonLinearity);
		return olsMultipleLinearRegression;
	} 
	private double[] giveSomeNonLinearBehaviour(double[] factorReturnFor1Factor)
	{
		double factorReturnsWithNonLinearBehaviour [] = new double[factorReturnFor1Factor.length*3];
		for(int i=0;i<factorReturnFor1Factor.length;i++) 
		{
			factorReturnsWithNonLinearBehaviour[i] = factorReturnFor1Factor[i];
			factorReturnsWithNonLinearBehaviour[i+factorReturnFor1Factor.length] = Math.signum(factorReturnFor1Factor[i])*factorReturnFor1Factor[i]*factorReturnFor1Factor[i];
			factorReturnsWithNonLinearBehaviour[i+factorReturnFor1Factor.length*2] = Math.signum(factorReturnFor1Factor[i])*Math.sqrt(Math.abs(factorReturnFor1Factor[i]));
		}
		return factorReturnsWithNonLinearBehaviour;
	}
	
	public  double sum(Double...values) {
		   double result = 0;
		   for (double value:values)
		     result += value;
		   return result;
		 }
	
	/* this should give transpose of 2D array formed after conversion of List to double.*/
	
	private double[][] getArrFromList(List<Double[]> factorsDailyReturn)
	{
		double[][] factorsDailyReturnArr = new double[factorsDailyReturn.size()][];
		int i = 0;
		for(Double[] arr : factorsDailyReturn)
		{
			factorsDailyReturnArr[i++]= ArrayUtils.toPrimitive(arr);
		}
		
		RealMatrix m = MatrixUtils.createRealMatrix(factorsDailyReturnArr);
		return m.transpose().getData();
	}
	
	private List<Double[]> getFactorsReturn(LocalDate businessDate)
	{
		List<Double[]> factorReturns = new ArrayList<Double[]>();
		
		LocalDate startDate = businessDate.minusDays(historicalDaysNeeded);
		List<String> marketFactorList = new ArrayList<String>();
		
		try {
			marketFactorList = MarketDataHistoryReader.getMarketFactorList();
			
			for(String symbol : marketFactorList)		
			{
				log.log(Level.INFO, "Symbol "+symbol);	
				factorReturns.add(historicalMarketDataStore.dailyReturns(symbol, startDate, businessDate));
			} 
			 } catch (IOException e) {
			e.printStackTrace();
		} 
		return factorReturns;
	}
	
	private List<StockReturn> getStocksReturn(LocalDate businessDate)
	{
		List<StockReturn> instrumentReturns = new ArrayList<StockReturn>();
		LocalDate startDate = businessDate.minusDays(historicalDaysNeeded);
		List<String> stocksList = new ArrayList<String>();
		/*stocksList.add("TFSC");
		stocksList.add("EGHT");
		stocksList.add("ARAY");*/
		try {
			stocksList = TradeDataReader.getStocksList();/*Files.readAllLines(Paths.get(data_dir, "StocksList.txt"));*/
			for(String symbol : stocksList)		
			{
				log.log(Level.INFO, "Symbol "+symbol);	
				instrumentReturns.add(new StockReturn(symbol,historicalMarketDataStore.dailyReturns(symbol, startDate, businessDate)));
			} 
		} catch (IOException e) {
			e.printStackTrace();
		} 
		return instrumentReturns;
	}
	
	
	
	
	public static void main(String[] args)
	{
		
		 SparkConf conf = new SparkConf();
		 conf.setAppName("Return Simulator");
		 if(!(System.getProperty("env", "aws")).equals("aws")){
		 conf.setMaster("local[3]");
		 conf.setExecutorEnv("spark.executor.memory", "3g");
		 }
		 if(args.length != 0)
		 {
			 parallelism = Integer.parseInt(args[0]);
			 System.out.println("parallelism "+parallelism );
		 }
		 
		 ReturnSimulator simulator = new ReturnSimulator(conf);
		simulator.start(LocalDate.parse("2016-03-17", DateTimeFormatter.ofPattern("yyyy-MM-dd",Locale.ENGLISH)));
	}
	
}
