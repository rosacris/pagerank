package com.miyagi;

import java.io.IOException;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.mortbay.log.Log;

public final class PagerankMapper extends TableMapper<ImmutableBytesWritable, DoubleWritable> {

	// HBase uses an atypical table layout.
	// I recommend taking a look at http://hbase.apache.org/book/datamodel.html for a more detailed explanation
	// In our case, the table layout looks like the following:
	
	// | Rowkey  | SuperColumn 'r' | SuperColumn 'd' |      
	// -----------------------------------------------
	// | urlId_A | "rank": value   | ulrId_B : "http://..."
	//                             | urlId_C : "http://..."
	//									...
	// -----------------------------------------------
	
	// In a nutshell: each row has a unique key, but there is an optional second level of indirection inside each super column (key/value).
	// In the example above, the row corresponding to urlId_A, contains in the super column r a single entry "rank" whose value
	// is the current rank. Moreover, it also have a super column d that contains a collection of key/value pairs corresponding to
	// the urls of the out links in urlId_A (the key is a long value computed from hashing the url in the value).
	// In other words, super column d is a map containing the out links of urlId_A.
	// In the algorithm below, we just use the RowKey, the "rank" of super column r, and the keys of super column d.
	
	// The input table is expected to have two colum families:
	private static final byte[] CF_URL = "d".getBytes();		// column family with long keys that identify the out links urls
	private static final byte[] CF_RANK = "r".getBytes();		// column family with a double value that represent the rank
	
	// The rank column family contains a single family qualifier with the rank.
	private static final byte[] QLF_RANK = "rank".getBytes();

	@Override
	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException{

		long urlId = Bytes.toLong(row.get());
		double rank = 1.0D;
		
		// Get the rank of the actual document if it has one (otherise the default value remains 1.0D).
		NavigableMap<byte[], byte[]> ranks = value.getFamilyMap(CF_RANK);
		if(ranks.size() > 0)
			rank = Bytes.toDouble(ranks.get(QLF_RANK));
		
		// Get the list of out links from document docId
		NavigableMap<byte[], byte[]> outLinks = value.getFamilyMap(CF_URL);

		// Get the number of out links in the document and compute the rank to share with them
		DoubleWritable sharedRank = new DoubleWritable(rank / outLinks.size());
		Log.info("urlId: " + urlId + " rank: " + rank + " #outlinks: " + outLinks.size());

		// For each url in the out links share some rank by writing to the context a row <url, shared rank>
		for(byte[] url: outLinks.keySet()){
			Log.info("url: " + Bytes.toLong(url) + " shared rank: " + sharedRank);
			context.write(new ImmutableBytesWritable(url), sharedRank);
		}
		
		// Finally share nothing with itself. This step is important because we ensure that
		// in the reduce phase at least the default rank will be computed for it (if no other
		// document points to it).
		context.write(row, new DoubleWritable(0));
	}
}
