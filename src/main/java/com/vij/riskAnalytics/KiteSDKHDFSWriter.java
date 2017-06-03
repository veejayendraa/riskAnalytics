package com.vij.riskAnalytics;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.Format;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.mapreduce.DatasetKeyOutputFormat;
import scala.Tuple2;



public class KiteSDKHDFSWriter {

	public static <T, V> void write(Class<T> type, Format f, String outPutDir, JavaPairRDD<V,T> rdd) throws IOException
	{
		Job job = Job.getInstance();
		 PartitionStrategy partitionStrategy = new PartitionStrategy.Builder()
			        .identity("businessDate", "business_date")
			        .identity("portfolio", "book")
			        .build();
		 
		 DatasetDescriptor writeDescriptor = new DatasetDescriptor.Builder()
			        .schema(type)
			        .format(f)
			        .partitionStrategy(partitionStrategy)
			        .build();
		
		 Dataset<T> dataSet = Datasets.create("dataset:file:"+outPutDir, writeDescriptor, type);
		 
		 DatasetKeyOutputFormat.configure(job).writeTo(dataSet).withType(type);	
		 JavaPairRDD<T, Void> finalRDD =  rdd.mapToPair(row-> new Tuple2<T,Void>(row._2,null));
		 finalRDD.saveAsNewAPIHadoopDataset(job.getConfiguration());
	}
}
