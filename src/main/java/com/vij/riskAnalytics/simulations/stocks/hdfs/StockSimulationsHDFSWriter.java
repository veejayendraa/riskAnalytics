package com.vij.riskAnalytics.simulations.stocks.hdfs;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class StockSimulationsHDFSWriter implements Serializable {
	
	transient private SQLContext sqlContext;
	final private String businessDate;
	public StockSimulationsHDFSWriter(String businessDate,JavaSparkContext jsc)
	{
	 sqlContext = new SQLContext(jsc);
	 this.businessDate = businessDate;
	}
	
	private class RDDConverter implements Function<Row,Row>{

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public Row call(Row r) throws Exception {
			// TODO Auto-generated method stub
			return new GenericRow(new Object[]{businessDate ,r.getAs("SIMULATIONUUID"),r.getAs("SYMBOL"),r.getAs("STOCKRETURN")});
		}
		}
	
	
	public void write(DataFrame df )
	{
		df.show();
		df.printSchema();
		JavaRDD<Row> javaRDD = df.javaRDD().map(new RDDConverter());
		
		String schemaString = "BUSINESSDATE SIMULATIONUUID SYMBOL STOCKRETURN";
		List<StructField> fields = new ArrayList<StructField>();
		for (String fieldName: schemaString.split(" ")) {
			if(fieldName.equals("STOCKRETURN"))
			{
				fields.add(DataTypes.createStructField(fieldName, DataTypes.DoubleType, true));
			}
			else 
			{
				fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
			}
		}
		StructType schema = DataTypes.createStructType(fields);
		
		DataFrame newDf = sqlContext.createDataFrame(javaRDD,schema);
		
		newDf.printSchema();
		newDf.show();
		newDf.select(
				newDf.col("BUSINESSDATE"),
				 //functions.date_format(df.col("BUSINESSDATE"), "yyyyMMdd").as("BUSINESSDATE"),
				newDf.col("SIMULATIONUUID"),
				newDf.col("SYMBOL"),
				newDf.col("STOCKRETURN"))
		.write()
		.mode(SaveMode.Append)
		.partitionBy("BUSINESSDATE","SYMBOL")
		.parquet("D://junk/parquet/simulations");
		
				
	}


}
