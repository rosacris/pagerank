package com.miyagi;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.mortbay.log.Log;

public class Processor 
{
	public Processor(String inputTable, String targetTable) throws IOException, ClassNotFoundException, InterruptedException{

		Configuration config = HBaseConfiguration.create();
//		Adjust here HBase configuration (probably a local running cluster) 
//		config.set("hbase.master","c2jobtracker:60000");
//		config.set("hbase.rootdir","hdfs://c2namenode:9000/hbase");
//		config.set("hbase.cluster.distributed", "true");
//		config.set("hbase.zookeeper.leaderport","3888");
//		config.set("hbase.zookeeper.property.clientPort","2181");
//		config.set("hbase.zookeeper.quorum", "c2slave1,c2slave2,c2slave3");
//		config.set("hbase.zookeeper.property.dataDir","/var/lib/hbase/zookeeper");

		Job job = new Job(config, "PageRank Algorithm");
		job.setJarByClass(PagerankMapper.class);    // class that contains mapper

		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs


		// Set mapper job
		TableMapReduceUtil.initTableMapperJob(
				inputTable,				      		// input table
				scan,	          					// Scan instance to control CF and attribute selection
				PagerankMapper.class,   			// mapper class
				ImmutableBytesWritable.class,	    // mapper output key
				DoubleWritable.class,	          	// mapper output value
				job);

		// Set reducer job
		TableMapReduceUtil.initTableReducerJob(
				targetTable,      					// output table
				PagerankReducer.class,             // reducer class
				job);

		// Launch Job and wait for completion
		if (!job.waitForCompletion(true)) {
			throw new IOException("Error in job!");
		}

	}

	public static void main( String[] args ){
		if(args.length < 2){
			System.out.println("Usage: program <source table> <target table>");
		}else{
			try{
				new Processor(args[0], args[1]);
			}catch(Exception e){
				Log.info(e.getStackTrace().toString());
			}
		}
	}
}
