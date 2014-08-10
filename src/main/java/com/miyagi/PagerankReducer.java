package com.miyagi;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.mortbay.log.Log;

public final class PagerankReducer extends TableReducer<ImmutableBytesWritable, DoubleWritable, ImmutableBytesWritable>{

	private static final byte[] CF_RANK = "r".getBytes();
	private static final byte[] QLF_RANK = "rank".getBytes();

	private static final float damping = 0.85F;
	
	public void reduce(ImmutableBytesWritable key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {
		
		double inLinksRankTotal = 0;
		
		// Add all the shared ranks from incoming links
		for(DoubleWritable sharedRank: values)
			inLinksRankTotal += sharedRank.get();
		
		// Compute the new rank of the doc
		double newRank = damping * inLinksRankTotal + (1 - damping);
		
		// Update the row of the url with the new rank
		Put p = new Put(key.get());
		p.add(CF_RANK, QLF_RANK, Bytes.toBytes(newRank));
		Log.info("urlId: " + Bytes.toLong(key.get()) + " Rank: " + newRank);
		context.write(null, p);
	}
}
